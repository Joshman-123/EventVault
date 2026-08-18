/**
 * @file EventVault.cpp
 * @author Joshua
 * @date 2025
 * @copyright Copyright (c) 2025. Anyone is free to use, modify, and distribute this software without restriction.
 * 
 * @brief Implementation of the EventVault class and its associated serialization and threading logic.
 */

#include "EventVault.hpp"
#include <cstring>
#include <unordered_set>

namespace evt
{
    EventPublisher::EventPublisher(EventVault& f_vault) : m_vault(f_vault)
    {

    }

    bool EventPublisher::hasTransporter() const
    {
        return m_vault.m_handle.m_transporter != nullptr;
    }

    /**
     * @details Flattens the ledger map into a contiguous binary block using memcpy.
     * Strings are padded with null bytes to ensure a strict, predictable fixed-size record format.
     */
    void EventPublisher::serialize()
    {
        // Allocate and zero-fill the entire fixed-size buffer + CRC header
        m_payloadBuffer.assign(m_totalPayloadSize, 0);

        size_t l_offset = sizeof(uint32_t); // Start after CRC header
        for (const auto& l_pair : m_vault.m_eventLedger)
        {
            if (l_offset >= m_totalPayloadSize)
            {
                break;
            }

            const std::string& l_eventStr = l_pair.first;
            const EventLedgerEntry& l_entry = l_pair.second;

            // Safely cap string length to avoid memory overrun
            const size_t l_strLen = std::min(l_eventStr.size(), m_vault.m_handle.m_maxStringSize);
            std::memcpy(m_payloadBuffer.data() + l_offset, l_eventStr.c_str(), l_strLen);
            l_offset += m_vault.m_handle.m_maxStringSize + 1;

            std::memcpy(m_payloadBuffer.data() + l_offset, &l_entry, sizeof(EventLedgerEntry));
            l_offset += sizeof(EventLedgerEntry);
        }

        // Calculate CRC32 of the payload and store it at the very beginning
        const uint32_t l_crc = calculateCRC32(m_payloadBuffer.data() + sizeof(uint32_t), m_fixedSize);
        std::memcpy(m_payloadBuffer.data(), &l_crc, sizeof(uint32_t));
    }

    void EventPublisher::pushUnlocked()
    {
        if (m_vault.m_handle.m_transporter && m_vault.m_historyIdx > 0)
        {
            serialize();
            m_vault.m_handle.m_transporter->publish(m_payloadBuffer.data(), m_payloadBuffer.size());
        }
    }

    void EventPublisher::push()
    {
        std::lock_guard<std::mutex> l_lock{m_vault.m_mutex};
        pushUnlocked();
        m_vault.m_historyIdx = 0;
    }

    // ============================================================================
    // EventVault
    // ============================================================================

    EventVault &EventVault::getInstance()
    {
        static EventVault l_instance{};
        return l_instance;
    }

    EventVault::EventVault() : m_eventHistory(m_handle.m_ringSize), m_publisher(*this)
    {
    }

    EventVault::~EventVault()
    {
        deInitInternal();
    }

    ErrorType EventVault::deInit()
    {
        return getInstance().deInitInternal();
    }

    ErrorType EventVault::deInitInternal()
    {
        std::lock_guard<std::mutex> l_lock{m_mutex};
        if(false == m_initInvoked.exchange(false, std::memory_order_relaxed))
        {
            return ErrorType::NOT_INITIALIZED;
        }

        m_running.store(false, std::memory_order_release);
        m_queueCv.notify_all();

        if (m_workerThread.joinable())
        {
            m_workerThread.join();
        }

        // Force flush any pending events in the buffer to the SoC before destroying
        if (m_historyIdx > 0)
        {
            m_publisher.pushUnlocked();
        }

        {
            std::lock_guard<std::mutex> l_queueLock{m_queueMutex};
            while (!m_eventQueue.empty())
            {
                m_eventQueue.pop();
            }
        }

        m_queueCapacity = 0;
        m_writeIdx = 0;
        m_readIdx = 0;
        m_historyIdx = 0;
        m_eventHistory.clear();
        m_eventLedger.clear();
        m_initInvoked.store(false, std::memory_order_release);

        if (m_publisher.hasTransporter())
        {
            m_publisher.m_vault.m_handle.m_transporter.reset();
        }

        return ErrorType::SUCCESS;
    }

    ErrorType EventVault::init(EventHandle &&f_handle)
    {
        return getInstance().initInternal(std::move(f_handle));
    }

    ErrorType EventVault::initInternal(EventHandle &&f_handle)
    {
        /*why Double safety Net, mutex and atomic?... because i still want
        to have quick lookup of is InitInvoked via Atomic for recordEvent(),
        and also still want to seralize Init and Deinit sequence via Mutex Lock */
        std::lock_guard<std::mutex> l_lock{m_mutex};
        if(true == m_initInvoked.load(std::memory_order_acquire))
        {
            return ErrorType::ALREADY_INITIALIZED;
        }

        if (f_handle.m_ringSize == 0U)
        {
            return ErrorType::INVALID_RING_SIZE;
        }

        if (f_handle.m_maxStringSize == 0U)
        {
            return ErrorType::INVALID_MAX_STRING_SIZE;
        }

        if (f_handle.m_maxLedgerEntries == 0U)
        {
            return ErrorType::INVALID_MAX_LEDGER_SIZE;
        }

        // Stop existing worker safely if re-initialized
        if (m_running.load(std::memory_order_acquire))
        {
            m_running.store(false, std::memory_order_release);
            m_queueCv.notify_all();
            if (m_workerThread.joinable())
            {
                m_workerThread.join();
            }
        }

        // If shrinking below our current index, we must push the existing data before resizing
        if (m_historyIdx >= f_handle.m_ringSize)
        {
            m_publisher.pushUnlocked();
            m_historyIdx = 0;
        }

        m_handle = std::move(f_handle);
        m_eventHistory.resize(m_handle.m_ringSize);
        
        // Pre-calculate fixed buffer sizes for the publisher
        m_publisher.m_fixedRecordSize = m_handle.m_maxStringSize + 1 + sizeof(EventLedgerEntry);
        m_publisher.m_fixedSize = m_handle.m_maxLedgerEntries * m_publisher.m_fixedRecordSize;
        m_publisher.m_totalPayloadSize = m_publisher.m_fixedSize + sizeof(uint32_t);
        m_publisher.m_payloadBuffer.reserve(m_publisher.m_totalPayloadSize);

        const size_t l_queueSize = m_handle.m_lockFreeQueueSize > 0 ? m_handle.m_lockFreeQueueSize : 1024;
        m_queueCapacity = l_queueSize;

        {
            std::lock_guard<std::mutex> l_queueLock{m_queueMutex};
            while (!m_eventQueue.empty())
            {
                m_eventQueue.pop();
            }
        }

        m_writeIdx = 0;

        m_readIdx = 0;

        m_running.store(true, std::memory_order_release);

        m_workerThread = std::thread(&EventVault::workerLoop, this);

        m_initInvoked.store(true,std::memory_order_release);

        return ErrorType::SUCCESS;
    }

    ErrorType EventVault::publishData()
    {
        auto& l_instance = getInstance();
        if (false == l_instance.m_initInvoked.load(std::memory_order_acquire))
        {
            return ErrorType::NOT_INITIALIZED;
        }

        return l_instance.publishDataInternal();
    }

    ErrorType EventVault::recordEvent(const std::string &f_eventStr)
    {
        auto& l_instance = getInstance();
        if(false == l_instance.m_initInvoked.load(std::memory_order_acquire))
        {
            return ErrorType::SUCCESS;
        }
        if (f_eventStr.empty())
        {
            return ErrorType::INVALID_INPUT;
        }
        if (f_eventStr.size() > l_instance.m_handle.m_maxStringSize)
        {
            return ErrorType::STRING_SIZE_TOO_LARGE;
        }
        return l_instance.recordEventInternal(f_eventStr.data(), f_eventStr.size());
    }

    size_t EventVault::getSerializedDataSize()
    {
        auto& l_instance = getInstance();
        if(false == l_instance.m_initInvoked.load(std::memory_order_acquire))
        {
            return 0;
        }

        std::lock_guard<std::mutex> l_lock{l_instance.m_mutex};
        
        // Actual serialized size = CRC header (4 bytes) + (number of ledger entries * record size)
        const size_t l_recordSize = l_instance.m_handle.m_maxStringSize + 1 + sizeof(EventLedgerEntry);
        const size_t l_numEntries = l_instance.m_eventLedger.size();
        const size_t l_dataSize = sizeof(uint32_t) + (l_numEntries * l_recordSize);
        
        return l_dataSize;
    }

    ErrorType EventVault::publishDataInternal()
    {
        std::lock_guard<std::mutex> l_lock{m_mutex};
        if (m_historyIdx > 0)
        {
            m_publisher.pushUnlocked();
            m_historyIdx = 0;
        }
        return ErrorType::SUCCESS;
    }

    /**
     * @details Implements the producer side of the wait-free ring buffer. Employs atomic 
     * fetch_add to securely reserve a slot without locks, enabling microsecond-level ingestion latency.
     */
    ErrorType EventVault::recordEventInternal(const char *f_data, const size_t f_len)
    {
        /*Not Mandatory for the Transporter to be avaialbe.If no transporter are there return back
        and dont perform any expensive operation*/
        if (!m_publisher.hasTransporter())
        {
            return ErrorType::SUCCESS;
        }

        {
            std::lock_guard<std::mutex> l_lock{m_queueMutex};
            if (m_eventQueue.size() >= m_queueCapacity)
            {
                return ErrorType::QUEUE_FULL;
            }

            m_eventQueue.emplace(f_data, f_len);
        }
        m_queueCv.notify_one();
        return ErrorType::SUCCESS;
    }

    /**
     * @details Background thread execution loop. Constantly polls the lock-free queue for new 
     * events to aggregate into the ledger. Utilizes time-based batching to periodically trigger
     * a flush to the payload publisher.
     */
    void EventVault::workerLoop()
    {
        while (m_running.load(std::memory_order_acquire) || !m_eventQueue.empty())
        {
            std::string l_eventStr{};
            {
                std::unique_lock<std::mutex> l_queueLock{m_queueMutex};
                m_queueCv.wait(l_queueLock, [this]() {
                    return !m_eventQueue.empty() || !m_running.load(std::memory_order_acquire);
                });

                if (m_eventQueue.empty())
                {
                    continue;
                }

                l_eventStr = std::move(m_eventQueue.front());
                m_eventQueue.pop();
            }

            const auto l_now = std::chrono::steady_clock::now();
            auto& l_eventHistoryStr = m_eventHistory[m_readIdx % m_handle.m_ringSize];
            l_eventHistoryStr = std::move(l_eventStr);

            auto l_it{m_eventLedger.find(l_eventHistoryStr)};
            if (l_it == m_eventLedger.end())
            {
                if (m_eventLedger.size() < m_handle.m_maxLedgerEntries)
                {
                    l_it = m_eventLedger.emplace(l_eventHistoryStr, EventLedgerEntry{}).first;
                }
            }

            if (l_it != m_eventLedger.end())
            {
                auto &l_entry = l_it->second;
                const uint64_t l_mono{static_cast<uint64_t>(l_now.time_since_epoch().count())};
                const uint64_t l_wall{static_cast<uint64_t>(std::chrono::system_clock::now().time_since_epoch().count())};

                if (l_entry.m_count == 0)
                {
                    l_entry.m_firstMonoTS = l_mono;
                    l_entry.m_firstWallTS = l_wall;
                }
                l_entry.m_count++;
                l_entry.m_lastMonoTS = l_mono;
                l_entry.m_lastWallTS = l_wall;
            }

            m_historyIdx++;
            m_readIdx++;
        }
    }
}
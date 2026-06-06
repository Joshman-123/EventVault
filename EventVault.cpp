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

    void EventPublisher::serialize()
    {
        // Calculate fixed size representing the maximum possible payload size for the entire ledger
        size_t l_fixedRecordSize = m_vault.m_handle.m_maxStringSize + 1 + sizeof(EventLedgerEntry);
        size_t l_fixedSize = m_vault.m_handle.m_maxLedgerEntries * l_fixedRecordSize;
        
        // Allocate and zero-fill the entire fixed-size buffer
        m_payloadBuffer.assign(l_fixedSize, 0);

        size_t l_offset = 0;
        for (const auto& l_pair : m_vault.m_eventLedger)
        {
            if (l_offset >= l_fixedSize)
            {
                break;
            }

            const std::string& l_eventStr = l_pair.first;
            const EventLedgerEntry& l_entry = l_pair.second;

            // Safely cap string length to avoid memory overrun
            size_t l_strLen = std::min(l_eventStr.size(), m_vault.m_handle.m_maxStringSize);
            std::memcpy(m_payloadBuffer.data() + l_offset, l_eventStr.c_str(), l_strLen);
            l_offset += m_vault.m_handle.m_maxStringSize + 1;

            std::memcpy(m_payloadBuffer.data() + l_offset, &l_entry, sizeof(EventLedgerEntry));
            l_offset += sizeof(EventLedgerEntry);
        }
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
        {
            std::lock_guard<std::mutex> l_lock{m_mutex};
            if(false == m_initInvoked)
            {
                return ErrorType::NOT_INITIALIZED;
            }
        }

        m_initInvoked = false;
        m_running.store(false, std::memory_order_release);
        if (m_workerThread.joinable())
        {
            m_workerThread.join();
        }

        std::lock_guard<std::mutex> l_lock{m_mutex};

        // Force flush any pending events in the buffer to the SoC before destroying
        if (m_historyIdx > 0)
        {
            m_publisher.pushUnlocked();
        }

        if (m_lockFreeQueue)
        {
            for (size_t l_i{}; l_i < m_queueCapacity; ++l_i)
            {
                delete[] m_lockFreeQueue[l_i].m_data;
                m_lockFreeQueue[l_i].m_data = nullptr;
            }
            m_lockFreeQueue.reset();
        }

        m_queueCapacity = 0;
        m_writeIdx.store(0, std::memory_order_relaxed);
        m_readIdx = 0;
        m_historyIdx = 0;
        m_eventHistory.clear();
        m_eventLedger.clear();

        return ErrorType::SUCCESS;
    }

    ErrorType EventVault::init(EventHandle &&f_handle)
    {
        return getInstance().initInternal(std::move(f_handle));
    }

    ErrorType EventVault::initInternal(EventHandle &&f_handle)
    {
        {
            std::lock_guard<std::mutex> l_lock{m_mutex};
            if(true == m_initInvoked)
            {
                return ErrorType::ALREADY_INITIALIZED;
            }
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

        std::lock_guard<std::mutex> l_lock{m_mutex};

        // Stop existing worker safely if re-initialized
        if (m_running.load(std::memory_order_acquire))
        {
            m_running.store(false, std::memory_order_release);
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

        // Allocate and setup memory pool for lock-free queue
        size_t l_queueSize = m_handle.m_lockFreeQueueSize > 0 ? m_handle.m_lockFreeQueueSize : 1024;
        if (m_lockFreeQueue)
        {
            for (size_t l_i{}; l_i < m_queueCapacity; ++l_i)
            {
                delete[] m_lockFreeQueue[l_i].m_data;
            }
        }

        m_queueCapacity = l_queueSize;

        m_lockFreeQueue.reset(new LockFreeNode[m_queueCapacity]);

        for (size_t l_i{}; l_i < m_queueCapacity; ++l_i)
        {
            m_lockFreeQueue[l_i].m_data = new char[m_handle.m_maxStringSize + 1];
            m_lockFreeQueue[l_i].m_ready.store(false, std::memory_order_relaxed);
        }

        m_writeIdx.store(0, std::memory_order_relaxed);

        m_readIdx = 0;

        m_running.store(true, std::memory_order_release);

        m_workerThread = std::thread(&EventVault::workerLoop, this);

        m_initInvoked = true;

        return ErrorType::SUCCESS;
    }

    ErrorType EventVault::recordEvent(const std::string &f_eventStr)
    {
        auto& l_instance = getInstance();
        if(false == l_instance.m_initInvoked)
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

    ErrorType EventVault::recordEvent(std::string &&f_eventStr)
    {
        auto& l_instance = getInstance();

        if(false == l_instance.m_initInvoked)
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

    ErrorType EventVault::recordEvent(const char *f_eventStr)
    {
        auto& l_instance = getInstance();
        if(false == l_instance.m_initInvoked)
        {
            return ErrorType::SUCCESS;
        }

        if (!f_eventStr || f_eventStr[0] == '\0')
        {
            return ErrorType::INVALID_INPUT;
        }
        size_t l_len = std::strlen(f_eventStr);
        if (l_len > l_instance.m_handle.m_maxStringSize)
        {
            return ErrorType::STRING_SIZE_TOO_LARGE;
        }
        return l_instance.recordEventInternal(f_eventStr, l_len);
    }

    ErrorType EventVault::recordEventInternal(const char *f_data, const size_t f_len)
    {
        /*Not Mandatory for the Transporter to be avaialbe.If no transporter are there return back
        and dont perform any expensive operation*/
        if (!m_publisher.hasTransporter())
        {
            return ErrorType::SUCCESS;
        }

        const size_t l_idx = m_writeIdx.fetch_add(1, std::memory_order_relaxed) % m_queueCapacity;
        auto& l_node = m_lockFreeQueue[l_idx];

        // If the consumer hasn't read this node yet, drop event to keep wait-free performance
        if (l_node.m_ready.load(std::memory_order_acquire))
        {
            return ErrorType::QUEUE_FULL;
        }

        std::memcpy(l_node.m_data, f_data, f_len);
        l_node.m_len = f_len;

        l_node.m_ready.store(true, std::memory_order_release);
        return ErrorType::SUCCESS;
    }

    void EventVault::workerLoop()
    {
        auto l_lastPushTime = std::chrono::steady_clock::now();

        while (m_running.load(std::memory_order_acquire))
        {
            const size_t l_idx = m_readIdx % m_queueCapacity;

            auto& l_node = m_lockFreeQueue[l_idx];

            if (l_node.m_ready.load(std::memory_order_acquire))
            {
                // Reuse the existing string buffer in the history ring to prevent
                // frequent dynamic memory allocations (new/delete) in the worker thread loop.
                auto& l_eventStr = m_eventHistory[m_historyIdx];
                l_eventStr.assign(l_node.m_data, l_node.m_len);

                auto l_it{m_eventLedger.find(l_eventStr)};
                if (l_it == m_eventLedger.end())
                {
                    if (m_eventLedger.size() < m_handle.m_maxLedgerEntries)
                    {
                        l_it = m_eventLedger.emplace(l_eventStr, EventLedgerEntry{}).first;
                    }
                }

                if (l_it != m_eventLedger.end())
                {
                    auto &l_entry = l_it->second;
                    uint64_t l_mono{static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count())};
                    uint64_t l_wall{static_cast<uint64_t>(std::chrono::system_clock::now().time_since_epoch().count())};

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

                if (m_historyIdx >= m_handle.m_ringSize)
                {
                    m_publisher.pushUnlocked();
                    m_historyIdx = 0;
                    l_lastPushTime = std::chrono::steady_clock::now();
                }

                l_node.m_ready.store(false, std::memory_order_release);
                m_readIdx++;
            }
            else
            {
                auto l_now = std::chrono::steady_clock::now();
                if (std::chrono::duration_cast<std::chrono::milliseconds>(l_now - l_lastPushTime).count() >= m_handle.m_sleepDurationMs)
                {
                    if (m_historyIdx > 0)
                    {
                        m_publisher.pushUnlocked();
                        m_historyIdx = 0;
                    }
                    l_lastPushTime = std::chrono::steady_clock::now();
                }

                // Yield gracefully to prevent CPU pegging while idle
                std::this_thread::sleep_for(std::chrono::milliseconds(m_handle.m_sleepDurationMs));
            }
        }
    }
}
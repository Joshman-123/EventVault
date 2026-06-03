#include "EventVault.hpp"
#include <cstring>

namespace evt
{
    EventVault &EventVault::getInstance()
    {
        static EventVault l_instance{};
        return l_instance;
    }

    EventVault::EventVault() : m_eventHistory(m_config.m_ringSize)
    {
    }

    ErrorType EventVault::init(const EventConfig &f_config)
    {
        if (f_config.m_ringSize == 0U)
        {
            return ErrorType::INVALID_RING_SIZE;
        }
        if (f_config.m_maxStringSize == 0U)
        {
            return ErrorType::INVALID_MAX_STRING_SIZE;
        }
        if (f_config.m_maxLedgerEntries == 0U)
        {
            return ErrorType::INVALID_MAX_LEDGER_SIZE;
        }

        std::lock_guard<std::mutex> l_lock{m_mutex};

        // If shrinking below our current index, we must push the existing data before resizing
        if (m_historyIdx >= f_config.m_ringSize)
        {
            pushToSoCUnlocked();
            m_historyIdx = 0;
        }

        m_config = f_config;
        m_eventHistory.resize(m_config.m_ringSize);

        return ErrorType::SUCCESS;
    }

    void EventVault::pushToSoCUnlocked()
    {
        if (m_transporter && m_historyIdx > 0)
        {
            m_payloadBuffer.clear(); // O(1) reset, keeps previously allocated capacity

            // Pre-calculate capacity to avoid vector reallocations
            size_t l_totalBytes{};
            for (size_t l_i{}; l_i < m_historyIdx; l_i++)
            {
                l_totalBytes += m_eventHistory[l_i].m_eventStr.size() + 1;
            }
            m_payloadBuffer.reserve(l_totalBytes); // Only allocates if totalBytes > current capacity

            for (size_t l_i{}; l_i < m_historyIdx; l_i++)
            {
                const auto &l_event = m_eventHistory[l_i];
                m_payloadBuffer.insert(m_payloadBuffer.end(), l_event.m_eventStr.begin(), l_event.m_eventStr.end());
                m_payloadBuffer.push_back('\0'); // null terminator string delimiter
            }
            m_transporter->publish(m_payloadBuffer.data(), m_payloadBuffer.size());
        }
    }

    ErrorType EventVault::recordEvent(const std::string &f_eventStr)
    {
        if (f_eventStr.empty())
        {
            return ErrorType::INVALID_INPUT;
        }
        if (f_eventStr.size() > m_config.m_maxStringSize)
        {
            return ErrorType::STRING_SIZE_TOO_LARGE;
        }
        return recordEventInternal(f_eventStr);
    }

    ErrorType EventVault::recordEvent(std::string &&f_eventStr)
    {
        if (f_eventStr.empty())
        {
            return ErrorType::INVALID_INPUT;
        }
        if (f_eventStr.size() > m_config.m_maxStringSize)
        {
            return ErrorType::STRING_SIZE_TOO_LARGE;
        }
        return recordEventInternal(std::move(f_eventStr));
    }

    ErrorType EventVault::recordEvent(const char *f_eventStr)
    {
        if (!f_eventStr || f_eventStr[0] == '\0')
        {
            return ErrorType::INVALID_INPUT;
        }
        if (std::strlen(f_eventStr) > m_config.m_maxStringSize)
        {
            return ErrorType::STRING_SIZE_TOO_LARGE;
        }
        return recordEventInternal(std::string(f_eventStr));
    }

    void EventVault::pushToSoC()
    {
        std::lock_guard<std::mutex> l_lock{m_mutex};
        pushToSoCUnlocked();
        m_historyIdx = 0; // Manually reset if pushed externally
    }
}
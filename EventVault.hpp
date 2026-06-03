#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <cstdint>
#include <utility>
#include <unordered_map>
#include <chrono>

// Forward declaration for the transporter interface.
// The actual implementation should be provided by the user.
struct ITransporter
{
    virtual ~ITransporter() = default;
    virtual void publish(const uint8_t* data, size_t size) = 0;
};

namespace evt
{
    enum class ErrorType : uint8_t
    {
        SUCCESS,
        INVALID_INPUT,
        STRING_SIZE_TOO_LARGE,
        INVALID_RING_SIZE,
        INVALID_MAX_STRING_SIZE,
        INVALID_MAX_LEDGER_SIZE
    };

    struct EventLedgerEntry
    {
        uint32_t m_count{};
        uint64_t m_firstMonoTS{};
        uint64_t m_lastMonoTS{};
        uint64_t m_firstWallTS{};
        uint64_t m_lastWallTS{};
    };

    struct ErrorEvent
    {
        std::string m_eventStr{};
    };

    struct EventConfig
    {
        size_t m_maxLedgerEntries{256};
        size_t m_maxStringSize{64};
        size_t m_ringSize{10};
    };

    class EventVault
    {
    public:
        static EventVault &getInstance();

        ErrorType init(const EventConfig &f_config);

        ErrorType recordEvent(const std::string &f_eventStr);
        ErrorType recordEvent(std::string &&f_eventStr);
        ErrorType recordEvent(const char *f_eventStr);

        void pushToSoC();

    private:
        EventVault();
        ~EventVault() = default;
        EventVault(const EventVault &) = delete;
        EventVault &operator=(const EventVault &) = delete;

        void pushToSoCUnlocked();

        template <typename StringType>
        ErrorType recordEventInternal(StringType &&f_eventStr);

        std::mutex m_mutex;
        std::unordered_map<std::string, EventLedgerEntry> m_eventLedger{};
        EventConfig m_config{};
        std::vector<ErrorEvent> m_eventHistory;
        size_t m_historyIdx{};
        ITransporter *m_transporter{};
        std::vector<uint8_t> m_payloadBuffer{};
    };

    template <typename StringType>
    ErrorType EventVault::recordEventInternal(StringType &&f_eventStr)
    {
        std::lock_guard<std::mutex> l_lock{m_mutex};

        auto l_it{m_eventLedger.find(f_eventStr)};
        if (l_it == m_eventLedger.end())
        {
            if (m_eventLedger.size() < m_config.m_maxLedgerEntries)
            {
                l_it = m_eventLedger.emplace(f_eventStr, EventLedgerEntry{}).first;
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

        m_eventHistory[m_historyIdx] = {std::forward<StringType>(f_eventStr)};
        m_historyIdx++;

        if (m_historyIdx >= m_config.m_ringSize)
        {
            pushToSoCUnlocked();
            m_historyIdx = 0;
        }

        return ErrorType::SUCCESS;
    }
}
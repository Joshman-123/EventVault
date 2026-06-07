#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <cstdint>
#include <utility>
#include <unordered_map>
#include <chrono>
#include <memory>
#include <atomic>
#include <thread>

namespace evt
{
    struct ITransporter
    {
        virtual ~ITransporter() = default;
        virtual void publish(const uint8_t* data, size_t size) = 0;
    };

    enum class ErrorType : uint8_t
    {
        SUCCESS,
        INVALID_INPUT,
        ALREADY_INITIALIZED,
        NOT_INITIALIZED,
        STRING_SIZE_TOO_LARGE,
        INVALID_RING_SIZE,
        INVALID_MAX_STRING_SIZE,
        INVALID_MAX_LEDGER_SIZE,
        QUEUE_FULL
    };

#pragma pack(push, 1)
    struct EventLedgerEntry
    {
        uint32_t m_count{};
        uint64_t m_firstMonoTS{};
        uint64_t m_lastMonoTS{};
        uint64_t m_firstWallTS{};
        uint64_t m_lastWallTS{};
    };
#pragma pack(pop)

    struct EventHandle
    {
        size_t m_maxLedgerEntries{256};
        size_t m_maxStringSize{64};
        size_t m_ringSize{10};
        size_t m_lockFreeQueueSize{1024};
        uint32_t m_sleepDurationMs{100};
        std::unique_ptr<ITransporter> m_transporter{};
    };

    struct LockFreeNode
    {
        char* m_data{};
        size_t m_len{};
        std::atomic<bool> m_ready{false};
    };

    class EventVault; // Forward declaration

    class EventPublisher final
    {
        private:
        friend class EventVault;
        explicit EventPublisher(EventVault& f_vault);

        bool hasTransporter() const;

        void pushUnlocked();
        void push();

        void serialize();

        EventVault& m_vault;
        std::vector<uint8_t> m_payloadBuffer{};
        size_t m_fixedRecordSize{};
        size_t m_fixedSize{};
    };

    class EventVault final
    {
    public:
        static ErrorType init(EventHandle &&f_handle);
        static ErrorType deInit();
        static ErrorType recordEvent(const std::string &f_eventStr);
        static ErrorType recordEvent(std::string &&f_eventStr);
        static ErrorType recordEvent(const char *f_eventStr);
    private:
        static EventVault &getInstance();

        friend class EventPublisher;

        EventVault();
        ~EventVault();
        EventVault(const EventVault &) = delete;
        EventVault &operator=(const EventVault &) = delete;

        ErrorType initInternal(EventHandle &&f_handle);
        ErrorType deInitInternal();

        ErrorType recordEventInternal(const char *f_data, const size_t f_len);
        void workerLoop();

        std::mutex m_mutex{};
        std::unordered_map<std::string, EventLedgerEntry> m_eventLedger{};
        EventHandle m_handle{};
        std::vector<std::string> m_eventHistory;
        size_t m_historyIdx{};
        EventPublisher m_publisher;
        bool m_initInvoked{false};

        // Lock-free Producer-Consumer queue components
        std::unique_ptr<LockFreeNode[]> m_lockFreeQueue{};
        size_t m_queueCapacity{};
        std::atomic<size_t> m_writeIdx{0};
        size_t m_readIdx{0};
        std::atomic<bool> m_running{false};
        std::thread m_workerThread{};
    };
}
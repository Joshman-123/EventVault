/**
 * @file EventVault.hpp
 * @author Joshua
 * @date 2025
 * @copyright Copyright (c) 2025. Anyone is free to use, modify, and distribute this software without restriction.
 * 
 * @brief Defines the EventVault system for high-performance, concurrent event logging and aggregation.
 */

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
    class EventVault;
}

using eVault = evt::EventVault;

namespace evt
{
    /**
     * @brief Computes standard IEEE 802.3 CRC-32 checksum.
     * @param data Pointer to the binary data array.
     * @param length Size of the binary data in bytes.
     * @return The 32-bit CRC checksum.
     */
    inline uint32_t calculateCRC32(const uint8_t* data, size_t length) {
        uint32_t crc = 0xFFFFFFFF;
        for (size_t i = 0; i < length; ++i) {
            crc ^= data[i];
            for (int j = 0; j < 8; ++j) {
                crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
            }
        }
        return ~crc;
    }

    /**
     * @brief Interface for data transport mechanisms.
     * 
     * Implementations of this interface define how the serialized event ledger 
     * is transmitted (e.g., written to disk, sent over a network, etc.).
     */
    struct ITransporter
    {
        virtual ~ITransporter() = default;
        
        /**
         * @brief Publishes the serialized payload.
         * @param data Pointer to the binary payload buffer.
         * @param size Size of the payload buffer in bytes.
         */
        virtual void publish(const uint8_t* data, const size_t size) = 0;
    };

    /**
     * @brief Enumeration of possible error codes returned by EventVault operations.
     */
    enum class ErrorType : uint8_t
    {
        SUCCESS,                 /**< Operation completed successfully. */
        INVALID_INPUT,           /**< Provided input (e.g., event string) was null or empty. */
        ALREADY_INITIALIZED,     /**< Vault initialization was attempted while already running. */
        NOT_INITIALIZED,         /**< Operation attempted before vault was initialized. */
        STRING_SIZE_TOO_LARGE,   /**< Event string exceeds configured maximum length. */
        INVALID_RING_SIZE,       /**< Configured ring size is invalid (e.g., 0). */
        INVALID_MAX_STRING_SIZE, /**< Configured maximum string size is invalid. */
        INVALID_MAX_LEDGER_SIZE, /**< Configured maximum ledger size is invalid. */
        QUEUE_FULL               /**< Lock-free queue is full; event was dropped. */
    };

    /**
     * @brief Structure representing a single aggregated event record.
     */
    struct EventLedgerEntry
    {
        uint64_t m_firstMonoTS{}; /**< Steady clock timestamp of the first occurrence. */
        uint64_t m_lastMonoTS{};  /**< Steady clock timestamp of the most recent occurrence. */
        uint64_t m_firstWallTS{}; /**< System clock timestamp of the first occurrence. */
        uint64_t m_lastWallTS{};  /**< System clock timestamp of the most recent occurrence. */
        uint32_t m_count{};       /**< Total number of times this event occurred. */
        uint32_t m_padding{};     /**< Explicit padding to maintain strict 8-byte alignment boundaries. */
    };

    /**
     * @brief Configuration handle used to initialize the EventVault.
     */
    struct EventHandle
    {
        size_t m_maxLedgerEntries{0};                /**< Maximum number of unique events tracked in the ledger. */
        size_t m_maxStringSize{0};                    /**< Maximum length in characters for an event name. */
        size_t m_ringSize{0};                         /**< History buffer size for recycling memory allocations. */
        size_t m_lockFreeQueueSize{0};              /**< Maximum concurrent pending events in the wait-free queue. */
        std::unique_ptr<ITransporter> m_transporter{}; /**< Custom transport implementation for payload publishing. */
    };

    /**
     * @brief A single node within the lock-free producer-consumer queue.
     */
    struct LockFreeNode
    {
        char* m_data{};                   /**< Pre-allocated buffer for storing the raw event string. */
        size_t m_len{};                   /**< Length of the stored event string. */
        std::atomic<bool> m_ready{false}; /**< Flag indicating data is ready for the consumer to read. */
    };

    class EventVault; // Forward declaration

    /**
     * @brief Responsible for serializing and publishing the event ledger.
     * 
     * The EventPublisher takes the current state of the EventVault's ledger,
     * serializes it into a flat, fixed-size binary buffer, and transmits it 
     * using the configured ITransporter.
     */
    class EventPublisher final
    {
        friend class EventVault;

        /**
         * @brief Constructs an EventPublisher tied to a specific EventVault.
         * @param f_vault Reference to the parent EventVault instance.
         */
        explicit EventPublisher(EventVault& f_vault);

        /**
         * @brief Checks if a valid transporter is configured.
         * @return true if an ITransporter implementation is available, false otherwise.
         */
        bool hasTransporter() const;

        /**
         * @brief Serializes the ledger and pushes it via the transporter without locking.
         * 
         * @warning This function is not thread-safe. It assumes the caller has 
         * already acquired the necessary locks or is operating in a safe context 
         * (e.g., the dedicated worker thread).
         */
        void pushUnlocked();

        /**
         * @brief Thread-safe push operation. 
         * Acquires the parent vault's mutex before calling pushUnlocked().
         */
        void push();

        /**
         * @brief Serializes the event ledger into the internal payload buffer.
         * 
         * Iterates through the vault's ledger and packs the string names alongside 
         * their corresponding EventLedgerEntry structs into a fixed-size binary array.
         */
        void serialize();

        /** @brief Reference to the parent vault to access the ledger and configuration. */
        EventVault& m_vault;
        
        /** @brief Pre-allocated buffer holding the flattened binary representation of the ledger. */
        std::vector<uint8_t> m_payloadBuffer{};
        
        /** @brief Pre-calculated size in bytes of a single serialized ledger record. */
        size_t m_fixedRecordSize{};
        
        /** @brief Pre-calculated maximum total size in bytes of the serialized payload. */
        size_t m_fixedSize{};

        /** @brief Pre-calculated total size including the CRC32 header. */
        size_t m_totalPayloadSize{};
    };

    // ============================================================================
    // EventVault
    // ============================================================================

    /**
     * @brief Core singleton class responsible for ingesting, aggregating, and dispatching events.
     * 
     * The EventVault uses a lock-free producer-consumer queue to accept event logs 
     * from multiple threads with virtually zero latency. A dedicated background thread 
     * aggregates these events into a ledger and periodically pushes them out.
     */
    class EventVault final
    {
    public:
        /**
         * @brief Initializes the EventVault singleton.
         * @param f_handle Configuration settings and dependencies (e.g., transporter).
         * @return ErrorType::SUCCESS on success, or an appropriate error code.
         */
        static ErrorType init(EventHandle &&f_handle);

        /**
         * @brief Safely drains pending events, stops the worker thread, and deinitializes the vault.
         * @return ErrorType::SUCCESS on success.
         */
        static ErrorType deInit();

        /**
         * @brief Publishes the current aggregated ledger immediately.
         * The caller decides when to serialize and push the buffered ledger data.
         * @return ErrorType::SUCCESS on success, or NOT_INITIALIZED if the vault has not been started.
         */
        static ErrorType publishData();

        /**
         * @brief Logs an event by string reference.
         * @param f_eventStr The event message to log.
         * @return ErrorType::SUCCESS on success, or an appropriate error code.
         */
        static ErrorType recordEvent(const std::string &f_eventStr);
    private:
        /**
         * @brief Retrieves the singleton instance.
         * @return Reference to the internal EventVault instance.
         */
        static EventVault &getInstance();

        friend class EventPublisher;

        /** @brief Private constructor for singleton pattern. */
        EventVault();
        
        /** @brief Private destructor. Invokes deInitInternal(). */
        ~EventVault();
        
        EventVault(EventVault &&) = delete;
        EventVault &operator=(EventVault &&) = delete;
        EventVault(const EventVault &) = delete;
        EventVault &operator=(const EventVault &) = delete;

        /**
         * @brief Internal implementation of the initialization logic.
         * @param f_handle Configuration settings and dependencies.
         * @return ErrorType::SUCCESS on success.
         */
        ErrorType initInternal(EventHandle &&f_handle);
        
        /**
         * @brief Internal implementation of the de-initialization logic.
         * @return ErrorType::SUCCESS on success.
         */
        ErrorType deInitInternal();

        /**
         * @brief Internal implementation for logging an event.
         * @param f_data Raw C-string event message.
         * @param f_len Length of the event message.
         * @return ErrorType::SUCCESS on success, or QUEUE_FULL.
         */
        ErrorType recordEventInternal(const char *f_data, const size_t f_len);

        /**
         * @brief Publishes the current ledger immediately from the caller context.
         * @return ErrorType::SUCCESS on success.
         */
        ErrorType publishDataInternal();
        
        /**
         * @brief Main execution loop for the background worker thread.
         */
        void workerLoop();

        std::mutex m_mutex{};                                              /**< Mutex protecting shared state during init/deInit. */
        std::unordered_map<std::string, EventLedgerEntry> m_eventLedger{}; /**< Ledger mapping unique event names to their aggregated records. */
        EventHandle m_handle{};                                            /**< Active configuration settings for the vault. */
        std::vector<std::string> m_eventHistory;                           /**< Ring buffer of pre-allocated strings to prevent runtime allocations. */
        size_t m_historyIdx{};                                             /**< Current index in the event history ring buffer. */
        EventPublisher m_publisher;                                        /**< Dedicated publisher instance for serializing/pushing data. */
        std::atomic<bool> m_initInvoked{false};                                         /**< Flag indicating if the vault has been initialized. */

        // Lock-free Producer-Consumer queue components
        std::unique_ptr<LockFreeNode[]> m_lockFreeQueue{}; /**< Fixed-size array representing the wait-free queue. */
        size_t m_queueCapacity{};                          /**< Total capacity of the lock-free queue. */
        std::atomic<size_t> m_writeIdx{0};                 /**< Producer's write index into the lock-free queue. */
        size_t m_readIdx{0};                               /**< Consumer's read index into the lock-free queue. */
        std::atomic<bool> m_running{false};                /**< Atomic flag controlling the lifecycle of the worker thread. */
        std::thread m_workerThread{};                      /**< Background thread responsible for aggregating events. */
    };
}
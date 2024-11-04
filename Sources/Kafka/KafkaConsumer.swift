//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Logging
import NIOConcurrencyHelpers
import NIOCore
import ServiceLifecycle

// MARK: - KafkaConsumerEventsDelegate

/// `NIOAsyncSequenceProducerDelegate` for ``KafkaConsumerEvents``.
internal struct KafkaConsumerEventsDelegate: Sendable {
    let stateMachine: NIOLockedValueBox<KafkaConsumer.StateMachine>
}

extension KafkaConsumerEventsDelegate: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        return // no backpressure
    }

    func didTerminate() {
        return // We have to call poll for events anyway, nothing to do here
    }
}

// MARK: - KafkaConsumerEvents

/// `AsyncSequence` implementation for handling ``KafkaConsumerEvent``s emitted by Kafka.
public struct KafkaConsumerEvents: Sendable, AsyncSequence {
    public typealias Element = KafkaConsumerEvent
    typealias BackPressureStrategy = NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure
    typealias WrappedSequence = NIOAsyncSequenceProducer<Element, BackPressureStrategy, KafkaConsumerEventsDelegate>
    let wrappedSequence: WrappedSequence

    /// `AsynceIteratorProtocol` implementation for handling ``KafkaConsumerEvent``s emitted by Kafka.
    public struct AsyncIterator: AsyncIteratorProtocol {
        var wrappedIterator: WrappedSequence.AsyncIterator

        public mutating func next() async -> Element? {
            await self.wrappedIterator.next()
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return AsyncIterator(wrappedIterator: self.wrappedSequence.makeAsyncIterator())
    }
}

// MARK: - KafkaConsumerMessages

/// `AsyncSequence` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
public struct KafkaConsumerMessages: Sendable, AsyncSequence {
    typealias LockedMachine = NIOLockedValueBox<KafkaConsumer.StateMachine>
    
    let stateMachine: LockedMachine
    let pollInterval: Duration
    let enablePartitionEof: Bool

    public typealias Element = KafkaConsumerMessage

    /// `AsynceIteratorProtocol` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
    public struct AsyncIterator: AsyncIteratorProtocol {
        private let stateMachine: MachineHolder
        let pollInterval: Duration
        let enablePartitionEof: Bool
        
        weak var cachedClient: RDKafkaClient?
        var messagesCount: Int = 0
        
        private final class MachineHolder: Sendable { // only for deinit
            let stateMachine: LockedMachine
            init(stateMachine: LockedMachine) {
                self.stateMachine = stateMachine
            }

            deinit {
                self.stateMachine.withLockedValue { $0.finishMessageConsumption() }
            }
        }
        
        init(stateMachine: LockedMachine, pollInterval: Duration, enablePartitionEof: Bool) {
            self.stateMachine = .init(stateMachine: stateMachine)
            self.pollInterval = pollInterval
            self.enablePartitionEof = enablePartitionEof
        }

        public mutating func next() async throws -> Element? {
            defer {
                self.messagesCount += 1
                if self.messagesCount >= 100 {
                    self.cachedClient = nil
                    self.messagesCount = 0
                }
            }
            while !Task.isCancelled {
                if let client = self.cachedClient, // fast path
                   let message = try client.consumerPoll() {
                    if !message.eof || self.enablePartitionEof {
                        return message
                    } else {
                        continue
                    }
                }
                let action = self.stateMachine.stateMachine.withLockedValue { $0.nextConsumerPollLoopAction() }
                switch action {
                case .poll(let client):
                    let message = try client.consumerPoll()
                    guard let message else {
                        self.cachedClient = nil
                        self.messagesCount = 0
                        try await Task.sleep(for: self.pollInterval)
                        continue
                    }
                    if message.eof && !self.enablePartitionEof {
                        continue
                    }
                    self.cachedClient = client
                    return message
                case .suspendPollLoop:
                    try await Task.sleep(for: self.pollInterval)
                case .terminatePollLoop:
                    return nil
                }
            }
            return nil
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return AsyncIterator(
            stateMachine: self.stateMachine,
            pollInterval: self.pollInterval,
            enablePartitionEof: self.enablePartitionEof
        )
    }
}

// MARK: - KafkaConsumer

/// A ``KafkaConsumer `` can be used to consume messages from a Kafka cluster.
public final class KafkaConsumer: Sendable, Service {
    typealias ProducerEvents = NIOAsyncSequenceProducer<
        KafkaConsumerEvent,
        NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure,
        KafkaConsumerEventsDelegate
    >

    /// The configuration object of the consumer client.
    private let configuration: KafkaConsumerConfiguration
    /// A logger.
    private let logger: Logger
    /// State of the `KafkaConsumer`.
    private let stateMachine: NIOLockedValueBox<StateMachine>
    
    /// An asynchronous sequence containing messages from the Kafka cluster.
    public let messages: KafkaConsumerMessages

    // Private initializer, use factory method or convenience init to create KafkaConsumer
    /// Initialize a new ``KafkaConsumer``.
    /// To listen to incoming messages, please subscribe to a list of topics using ``subscribe()``
    /// or assign the consumer to a particular topic + partition pair using ``assign(topic:partition:offset:)``.
    ///
    /// - Parameters:
    ///     - client: Client used for handling the connection to the Kafka cluster.
    ///     - stateMachine: The state machine containing the state of the ``KafkaConsumer``.
    ///     - configuration: The ``KafkaConsumerConfiguration`` for configuring the ``KafkaConsumer``.
    ///     - logger: A logger.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    private init(
        client: RDKafkaClient,
        stateMachine: NIOLockedValueBox<StateMachine>,
        configuration: KafkaConsumerConfiguration,
        logger: Logger,
        eventSource: ProducerEvents.Source? = nil
    ) throws {
        self.configuration = configuration
        self.stateMachine = stateMachine
        self.logger = logger

        self.messages = KafkaConsumerMessages(
            stateMachine: self.stateMachine,
            pollInterval: configuration.pollInterval,
            enablePartitionEof: configuration.enablePartitionEof
        )

        self.stateMachine.withLockedValue {
            $0.initialize(
                client: client,
                eventSource: eventSource
            )
        }
    }

    /// Initialize a new ``KafkaConsumer``.
    ///
    /// This creates a consumer without that does not listen to any events other than consumer messages.
    ///
    /// - Parameters:
    ///     - configuration: The ``KafkaConsumerConfiguration`` for configuring the ``KafkaConsumer``.
    ///     - logger: A logger.
    /// - Returns: The newly created ``KafkaConsumer``.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    public convenience init(
        configuration: KafkaConsumerConfiguration,
        logger: Logger
    ) throws {
        var subscribedEvents: [RDKafkaEvent] = [.log]

        // Only listen to offset commit events when autoCommit is false
        if configuration.isAutoCommitEnabled == false {
            subscribedEvents.append(.offsetCommit)
        }
        if configuration.metrics.enabled {
            subscribedEvents.append(.statistics)
        }

        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: configuration.dictionary,
            events: subscribedEvents,
            logger: logger
        )

        let stateMachine = NIOLockedValueBox(StateMachine())

        try self.init(
            client: client,
            stateMachine: stateMachine,
            configuration: configuration,
            logger: logger
        )
    }

    /// Initialize a new ``KafkaConsumer`` and a ``KafkaConsumerEvents`` asynchronous sequence.
    ///
    /// Use the asynchronous sequence to consume events.
    ///
    /// - Important: When the asynchronous sequence is deinited the producer will be shut down and disallowed from sending more messages.
    /// Additionally, make sure to consume the asynchronous sequence otherwise the events will be buffered in memory indefinitely.
    ///
    /// - Parameters:
    ///     - configuration: The ``KafkaConsumerConfiguration`` for configuring the ``KafkaConsumer``.
    ///     - logger: A logger.
    /// - Returns: A tuple containing the created ``KafkaConsumer`` and the ``KafkaConsumerEvents``
    /// `AsyncSequence` used for receiving message events.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    public static func makeConsumerWithEvents(
        configuration: KafkaConsumerConfiguration,
        logger: Logger
    ) throws -> (KafkaConsumer, KafkaConsumerEvents) {
        var subscribedEvents: [RDKafkaEvent] = [.log]
        // Only listen to offset commit events when autoCommit is false
        if configuration.isAutoCommitEnabled == false {
            subscribedEvents.append(.offsetCommit)
        }
        if configuration.metrics.enabled {
            subscribedEvents.append(.statistics)
        }
//        NOTE: since now consumer is being polled with rd_kafka_consumer_poll,
//        we have to listen for rebalance through callback, otherwise consumer may fail
//        if configuration.listenForRebalance {
//            subscribedEvents.append(.rebalance)
//        }
        
        // we assign events once, so it is always thread safe -> @unchecked Sendable
        // but before start of consumer
        final class EventsInFutureWrapper: @unchecked Sendable {
            weak var consumer: KafkaConsumer? = nil
        }
        
        let wrapper = EventsInFutureWrapper()
        
        // as kafka_consumer_poll is used, we MUST define rebalance cb instead of listening to events
        let rebalanceCallBackStorage: RDKafkaClient.RebalanceCallbackStorage?
        if configuration.listenForRebalance {
            rebalanceCallBackStorage = RDKafkaClient.RebalanceCallbackStorage { rebalanceEvent in
                let action = wrapper.consumer?.stateMachine.withLockedValue { $0.nextEventPollLoopAction() }
                switch action {
                case .pollForEvents(_, let eventSource):
                    // FIXME: in fact, it is better to put to messages sequence
                    // but so far there is no particular design for rebalance
                    // so, let's put it to events as previously
                    _ = eventSource?.yield(.init(rebalanceEvent))
                default:
                    return
                }
            }
        } else {
            rebalanceCallBackStorage = nil
        }

        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: configuration.dictionary,
            events: subscribedEvents,
            logger: logger,
            rebalanceCallBackStorage: rebalanceCallBackStorage
        )

        let stateMachine = NIOLockedValueBox(StateMachine())
        
        // Note:
        // It's crucial to initialize the `sourceAndSequence` variable AFTER `client`.
        // This order is important to prevent the accidental triggering of `KafkaConsumerCloseOnTerminate.didTerminate()`.
        // If this order is not met and `RDKafkaClient.makeClient()` fails,
        // it leads to a call to `stateMachine.messageSequenceTerminated()` while it's still in the `.uninitialized` state.
        let sourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: KafkaConsumerEvent.self,
            backPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure(),
            finishOnDeinit: true,
            delegate: KafkaConsumerEventsDelegate(stateMachine: stateMachine)
        )
        
        let consumer = try KafkaConsumer(
            client: client,
            stateMachine: stateMachine,
            configuration: configuration,
            logger: logger,
            eventSource: sourceAndSequence.source
        )
        wrapper.consumer = consumer

        let eventsSequence = KafkaConsumerEvents(wrappedSequence: sourceAndSequence.sequence)
        return (consumer, eventsSequence)
    }

    /// Subscribe to the given list of `topics`.
    /// The partition assignment happens automatically using `KafkaConsumer`'s consumer group.
    /// - Parameter topics: An array of topic names to subscribe to.
    /// - Throws: A ``KafkaError`` if subscribing to the topic list failed.
    private func subscribe(topics: [String]) throws {
        let action = self.stateMachine.withLockedValue { $0.setUpConnection() }
        if topics.isEmpty {
            return
        }
        switch action {
        case .setUpConnection(let client):
            let subscription = RDKafkaTopicPartitionList()
            for topic in topics {
                subscription.add(
                    topic: topic,
                    partition: KafkaPartition.unassigned
                )
            }
            try client.subscribe(topicPartitionList: subscription)
        case .consumerClosed:
            throw KafkaError.connectionClosed(reason: "Consumer deinitialized before setup")
        }
    }

    /// Assign the``KafkaConsumer`` to a specific `partition` of a `topic`.
    /// - Parameter topic: Name of the topic that this ``KafkaConsumer`` will read from.
    /// - Parameter partition: Partition that this ``KafkaConsumer`` will read from.
    /// - Parameter offset: The offset to start consuming from.
    /// Defaults to the end of the Kafka partition queue (meaning wait for next produced message).
    /// - Throws: A ``KafkaError`` if the consumer could not be assigned to the topic + partition pair.
    private func assign(
        partitions: [KafkaConsumerConfiguration.ConsumptionStrategy.TopicPartition]
    ) async throws {
        let action = self.stateMachine.withLockedValue { $0.setUpConnection() }
        switch action {
        case .setUpConnection(let client):
            let assignment = RDKafkaTopicPartitionList()
            for partition in partitions {
                assignment.setOffset(topic: partition.topic, partition: partition.partition, offset: partition.offset)
            }
            try await client.assign(topicPartitionList: assignment)
        case .consumerClosed:
            throw KafkaError.connectionClosed(reason: "Consumer deinitialized before setup")
        }
    }
    
    /// Subscribe to the given list of `topics`.
    /// The partition assignment happens automatically using `KafkaConsumer`'s consumer group.
    /// - Parameter topics: An array of topic names to subscribe to.
    /// - Throws: A ``KafkaError`` if subscribing to the topic list failed.
    public func subscribeTopics(topics: [String]) throws {
        if topics.isEmpty {
            return
        }
        let client = try self.stateMachine.withLockedValue { try $0.client() }
        let subscription = RDKafkaTopicPartitionList()
        for topic in topics {
            subscription.add(topic: topic, partition: KafkaPartition.unassigned)
        }
        try client.subscribe(topicPartitionList: subscription)
    }
        
    
    public func assign(_ list: KafkaTopicList?) async throws {
        let action = self.stateMachine.withLockedValue { $0.seekOrRebalance() }
        switch action {
        case .allowed(let client):
            try await client.assign(topicPartitionList: list?.list)
        case .denied(let err):
            throw KafkaError.client(reason: err)
        }
    }
    
    public func incrementalAssign(_ list: KafkaTopicList) async throws {
        let action = self.stateMachine.withLockedValue { $0.seekOrRebalance() }
        switch action {
        case .allowed(let client):
            try await client.incrementalAssign(topicPartitionList: list.list)
        case .denied(let err):
            throw KafkaError.client(reason: err)
        }
    }
    
    public func incrementalUnassign(_ list: KafkaTopicList) async throws {
        let action = self.stateMachine.withLockedValue { $0.seekOrRebalance() }
        switch action {
        case .allowed(let client):
            try await client.incrementalUnassign(topicPartitionList: list.list)
        case .denied(let err):
            throw KafkaError.client(reason: err)
        }
    }
    
    // TODO: add docc: timeout = 0 -> async (no errors reported)
    public func seek(_ list: KafkaTopicList, timeout: Duration) async throws {
        let action = self.stateMachine.withLockedValue { $0.seekOrRebalance() }
        switch action {
        case .allowed(let client):
            try await client.seek(topicPartitionList: list.list, timeout: timeout)
        case .denied(let err):
            throw KafkaError.client(reason: err)
        }
    }
    
    public func metadata() async throws -> KafkaMetadata {
        try await client().metadata()
    }

    /// Start the ``KafkaConsumer``.
    ///
    /// - Important: This method **must** be called and will run until either the calling task is cancelled or gracefully shut down.
    public func run() async throws {
        try await withGracefulShutdownHandler {
            try await self._run()
        } onGracefulShutdown: {
            self.triggerGracefulShutdown()
        }
    }

    private func _run() async throws {
        switch self.configuration.consumptionStrategy._internal {
        case .partitions(_, let partitions):
            try await self.assign(partitions: partitions)
        case .group(groupID: _, topics: let topics):
            try self.subscribe(topics: topics)
        }
        try await self.eventRunLoop()
    }

    /// Run loop polling Kafka for new events.
    private func eventRunLoop() async throws {
        var pollInterval = configuration.pollInterval
        var events = [RDKafkaClient.KafkaEvent]()
        events.reserveCapacity(100)
        while !Task.isCancelled {
            let nextAction = self.stateMachine.withLockedValue { $0.nextEventPollLoopAction() }
            switch nextAction {
            case .pollForEvents(let client, let eventSource):
                // Event poll to serve any events queued inside of `librdkafka`.
                let shouldSleep = client.eventPoll(events: &events)
                for event in events {
                    switch event {
                    case .statistics(let statistics):
                        self.configuration.metrics.update(with: statistics)
                    case .rebalance(let rebalance):
                        self.logger.info("rebalance received \(rebalance), source nil: \(eventSource == nil)")
                        if let eventSource {
                            _ = eventSource.yield(.rebalance(rebalance))
                        } else {
                            try await client.assign(topicPartitionList: nil) // fallback
                        }
                    case .error(let error):
                        if let eventSource {
                            _ = eventSource.yield(.error(error))
                        } else {
                            throw error
                        }
                    default:
                        break // Ignore
                    }
                }
                if shouldSleep {
                    pollInterval = min(self.configuration.pollInterval, pollInterval * 2)
                    try await Task.sleep(for: pollInterval)
                } else {
                    pollInterval = max(pollInterval / 3, .microseconds(1))
                    await Task.yield()
                }
            case .terminatePollLoop:
                return
            }
        }
    }

    /// Mark all messages up to the passed message in the topic as read.
    /// Schedules a commit and returns immediately.
    /// Any errors encountered after scheduling the commit will be discarded.
    ///
    /// This method is only used for manual offset management.
    ///
    /// - Warning: This method fails if the ``KafkaConsumerConfiguration/isAutoCommitEnabled`` configuration property is set to `true` (default).
    ///
    /// - Parameters:
    ///     - message: Last received message that shall be marked as read.
    /// - Throws: A ``KafkaError`` if committing failed.
    public func scheduleCommit(_ message: KafkaConsumerMessage) throws {
        let action = self.stateMachine.withLockedValue { $0.commit() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to commit message offset on a closed consumer")
        case .commit(let client):
            guard self.configuration.isAutoCommitEnabled == false else {
                throw KafkaError.config(reason: "Committing manually only works if isAutoCommitEnabled set to false")
            }

            try client.scheduleCommit(message)
        }
    }

    @available(*, deprecated, renamed: "commit")
    public func commitSync(_ message: KafkaConsumerMessage) async throws {
        try await self.commit(message)
    }

    /// Mark all messages up to the passed message in the topic as read.
    /// Awaits until the commit succeeds or an error is encountered.
    ///
    /// This method is only used for manual offset management.
    ///
    /// - Warning: This method fails if the ``KafkaConsumerConfiguration/isAutoCommitEnabled`` configuration property is set to `true` (default).
    ///
    /// - Parameters:
    ///     - message: Last received message that shall be marked as read.
    /// - Throws: A ``KafkaError`` if committing failed.
    public func commit(_ message: KafkaConsumerMessage) async throws {
        let action = self.stateMachine.withLockedValue { $0.commit() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to commit message offset on a closed consumer")
        case .commit(let client):
            guard self.configuration.isAutoCommitEnabled == false else {
                throw KafkaError.config(reason: "Committing manually only works if isAutoCommitEnabled set to false")
            }

            try await client.commit(message)
        }
    }

    /// This function is used to gracefully shut down a Kafka consumer client.
    ///
    /// - Note: Invoking this function is not always needed as the ``KafkaConsumer``
    /// will already shut down when consumption of the ``KafkaConsumerMessages`` has ended.
    public func triggerGracefulShutdown() {
        let action = self.stateMachine.withLockedValue { $0.finish() }
        switch action {
        case .triggerGracefulShutdown(let client):
            self._triggerGracefulShutdown(
                client: client,
                logger: self.logger
            )
        case .none:
            return
        }
    }

    private func _triggerGracefulShutdown(
        client: RDKafkaClient,
        logger: Logger
    ) {
        do {
            try client.consumerClose()
        } catch {
            if let error = error as? KafkaError {
                logger.error("Closing KafkaConsumer failed: \(error.description)")
            } else {
                logger.error("Caught unknown error: \(error)")
            }
        }
    }

    func client() throws -> RDKafkaClient {
        return try self.stateMachine.withLockedValue { try $0.client() }
    }
}

// MARK: - KafkaConsumer + StateMachine

extension KafkaConsumer {
    /// State machine representing the state of the ``KafkaConsumer``.
    struct StateMachine: Sendable {
        /// The state of the ``StateMachine``.
        enum State: Sendable {
            /// The state machine has been initialized with init() but is not yet Initialized
            /// using `func initialize()` (required).
            case uninitialized
            /// We are in the process of initializing the ``KafkaConsumer``,
            /// though ``subscribe()`` / ``assign()`` have not been invoked.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: The source for yielding new messages.
            /// - Parameter eventSource: ``NIOAsyncSequenceProducer/Source`` used for yielding new events.
            case initializing(
                client: RDKafkaClient,
                eventSource: ProducerEvents.Source?
            )
            /// The ``KafkaConsumer`` is consuming messages.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter eventSource: ``NIOAsyncSequenceProducer/Source`` used for yielding new events.
            case running(client: RDKafkaClient, eventSource: ProducerEvents.Source?)
            /// The ``KafkaConsumer/triggerGracefulShutdown()`` has been invoked.
            /// We are now in the process of commiting our last state to the broker.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter eventSource: ``NIOAsyncSequenceProducer/Source`` used for yielding new events.
            case finishing(client: RDKafkaClient, eventSource: ProducerEvents.Source?)
            /// The ``KafkaConsumer`` is closed.
            case finished
        }

        /// The current state of the StateMachine.
        var state: State = .uninitialized

        /// Delayed initialization of `StateMachine` as the `source` and the `pollClosure` are
        /// not yet available when the normal initialization occurs.
        mutating func initialize(
            client: RDKafkaClient,
            eventSource: ProducerEvents.Source?
        ) {
            guard case .uninitialized = self.state else {
                fatalError("\(#function) can only be invoked in state .uninitialized, but was invoked in state \(self.state)")
            }
            self.state = .initializing(
                client: client,
                eventSource: eventSource
            )
        }

        /// Action to be taken when wanting to poll for a new message.
        enum EventPollLoopAction {
            /// The ``KafkaConsumer`` stopped consuming messages or
            /// is in the process of shutting down.
            /// Poll to serve any queued events and commit outstanding state to the broker.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter eventSource: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case pollForEvents(client: RDKafkaClient, eventSource: ProducerEvents.Source?)
            /// Terminate the poll loop.
            case terminatePollLoop
        }

        /// Returns the next action to be taken when wanting to poll.
        /// - Returns: The next action to be taken when wanting to poll, or `nil` if there is no action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        mutating func nextEventPollLoopAction() -> EventPollLoopAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before reading messages")
            case .running(let client, let eventSource):
                return .pollForEvents(client: client, eventSource: eventSource)
            case .finishing(let client, let eventSource):
                if client.isConsumerClosed {
                    self.state = .finished
                    return .terminatePollLoop
                } else {
                    return .pollForEvents(client: client, eventSource: eventSource)
                }
            case .finished:
                return .terminatePollLoop
            }
        }

        /// Action to be taken when wanting to poll for a new message.
        enum ConsumerPollLoopAction {
            /// Poll for a new ``KafkaConsumerMessage``.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case poll(client: RDKafkaClient)
            /// Sleep for ``KafkaConsumerConfiguration/pollInterval``.
            case suspendPollLoop
            /// Terminate the poll loop.
            case terminatePollLoop
        }

        /// Returns the next action to be taken when wanting to poll.
        /// - Returns: The next action to be taken when wanting to poll, or `nil` if there is no action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        mutating func nextConsumerPollLoopAction() -> ConsumerPollLoopAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                return .suspendPollLoop
            case .running(let client, _):
                return .poll(client: client)
            case .finishing, .finished:
                return .terminatePollLoop
            }
        }

        /// Action to be taken when wanting to set up the connection through ``subscribe()`` or ``assign()``.
        enum SetUpConnectionAction {
            /// Set up the connection through ``subscribe()`` or ``assign()``.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case setUpConnection(client: RDKafkaClient)
            /// The ``KafkaConsumer`` is closed.
            case consumerClosed
        }

        /// Get action to be taken when wanting to set up the connection through ``subscribe()`` or ``assign()``.
        ///
        /// - Returns: The action to be taken.
        mutating func setUpConnection() -> SetUpConnectionAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing(let client, let eventSource):
                self.state = .running(client: client, eventSource: eventSource)
                return .setUpConnection(client: client)
            case .running:
                fatalError("\(#function) should not be invoked more than once")
            case .finishing:
                fatalError("\(#function) should only be invoked when KafkaConsumer is running")
            case .finished:
                return .consumerClosed
            }
        }

        /// Action to take when wanting to store a message offset (to be auto-committed by `librdkafka`).
        enum StoreOffsetAction {
            /// Store the message offset with the given `client`.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case storeOffset(client: RDKafkaClient)
            /// The consumer is in the process of `.finishing` or even `.finished`.
            /// Stop yielding new elements and terminate the asynchronous sequence.
            case terminateConsumerSequence
        }

        /// Get action to take when wanting to store a message offset (to be auto-committed by `librdkafka`).
        func storeOffset() -> StoreOffsetAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before reading messages")
            case .running(let client, _):
                return .storeOffset(client: client)
            case .finishing, .finished:
                return .terminateConsumerSequence
            }
        }

        /// Action to be taken when wanting to do a commit.
        enum CommitAction {
            /// Do a commit.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case commit(client: RDKafkaClient)
            /// Throw an error. The ``KafkaConsumer`` is closed.
            case throwClosedError
        }

        /// Get action to be taken when wanting to do a commit.
        /// - Returns: The action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        func commit() -> CommitAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before reading messages")
            case .running(let client, _):
                return .commit(client: client)
            case .finishing, .finished:
                return .throwClosedError
            }
        }

        /// Action to be taken when wanting to do close the consumer.
        enum FinishAction {
            /// Shut down the ``KafkaConsumer``.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case triggerGracefulShutdown(client: RDKafkaClient)
        }

        /// Get action to be taken when wanting to do close the consumer.
        /// - Returns: The action to be taken,  or `nil` if there is no action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        mutating func finish() -> FinishAction? {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before reading messages")
            case .running(let client, let eventSource),
                 .finishing(let client, let eventSource):
                self.state = .finishing(client: client, eventSource: eventSource)
                return .triggerGracefulShutdown(client: client)
            case .finished:
                return nil
            }
        }
        
        enum RebalanceAction {
            /// Rebalance is still possible
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case allowed(
                client: RDKafkaClient
            )
            /// Throw an error. The ``KafkaConsumer`` is closed.
            case denied(error: String)
        }

        
        func seekOrRebalance() -> RebalanceAction {
            switch state {
            case .uninitialized:
                fatalError("\(#function) should not be invoked in state \(self.state)")
            case .initializing:
                fatalError("\(#function) should not be invoked in state \(self.state)")
            case .running(let client, _):
                return .allowed(client: client)
            case .finishing(let client, _):
                return .allowed(client: client)
            case .finished:
                return .denied(error: "Consumer finished")
            }
        }

        /// The ``KafkaConsumerMessages`` asynchronous sequence was terminated.
        mutating func finishMessageConsumption() {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                self.state = .finished
            case .running(let client, let eventSource):
                self.state = .finishing(client: client, eventSource: eventSource)
            case .finishing, .finished:
                break
            }
        }
        
        func client() throws -> RDKafkaClient {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing(let client, _):
                return client
            case .running(let client, _):
                return client
            case .finishing(let client, _):
                return client
            case .finished:
                throw KafkaError.client(reason: "Consumer is finished")
            }
        }
    }
}

//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import struct Foundation.UUID
import NIOCore
import ServiceLifecycle
@testable import SwiftKafka
import XCTest
import Logging

// For testing locally on Mac, do the following:
//
// 1. Install Kafka and Zookeeper using homebrew
//
// https://medium.com/@Ankitthakur/apache-kafka-installation-on-mac-using-homebrew-a367cdefd273
//
// 2. Start Zookeeper & Kafka Server
//
// (Homebrew - Apple Silicon)
// zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties & kafka-server-start /opt/homebrew/etc/kafka/server.properties
//
// (Homebrew - Intel Mac)
// zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties

final class SwiftKafkaTests: XCTestCase {
    // Read environment variables to get information about the test Kafka server
    let kafkaHost: String = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
    let kafkaPort: Int = .init(ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092")!
    var bootstrapServer: KafkaConfiguration.Broker!
    var producerConfig: KafkaProducerConfiguration!
    var uniqueTestTopic_: String? = nil
    var uniqueTestTopic: String {
        uniqueTestTopic_!
    }

    override func setUpWithError() throws {
        self.bootstrapServer = KafkaConfiguration.Broker(host: self.kafkaHost, port: self.kafkaPort)

        self.producerConfig = KafkaProducerConfiguration()
        self.producerConfig.bootstrapServers = [self.bootstrapServer]
        self.producerConfig.broker.addressFamily = .v4

        var basicConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "no-group", topics: [])
        )
        basicConfig.bootstrapServers = [self.bootstrapServer]
        basicConfig.broker.addressFamily = .v4

        // TODO: ok to block here? How to make setup async?
        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: basicConfig.dictionary,
            events: [],
            logger: .kafkaTest
        )
        self.uniqueTestTopic_ = try client._createUniqueTopic(timeout: 10 * 1000)
    }

    override func tearDownWithError() throws {
        var basicConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "no-group", topics: [])
        )
        basicConfig.bootstrapServers = [self.bootstrapServer]
        basicConfig.broker.addressFamily = .v4

        // TODO: ok to block here? Problem: Tests may finish before topic is deleted
        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: basicConfig.dictionary,
            events: [],
            logger: .kafkaTest
        )
        if let uniqueTestTopic_ {
            try client._deleteTopic(uniqueTestTopic_, timeout: 10 * 1000)
        }

        self.bootstrapServer = nil
        self.producerConfig = nil
        self.uniqueTestTopic_ = nil
    }

    func testProduceAndConsumeWithConsumerGroup() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(config: self.producerConfig, logger: .kafkaTest)

        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "subscription-test-group-id", topics: [self.uniqueTestTopic])
        )
        consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
        consumerConfig.bootstrapServers = [self.bootstrapServer]
        consumerConfig.broker.addressFamily = .v4

        let consumer = try KafkaConsumer(
            config: consumerConfig,
            logger: .kafkaTest
        )

        let serviceGroup = ServiceGroup(
            services: [producer, consumer],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    events: events,
                    messages: testMessages
                )
            }

            // Consumer Task
            group.addTask {
                var consumedMessages = [KafkaConsumerMessage]()
                for try await messageResult in consumer.messages {
                    guard case let message = messageResult else {
                        continue
                    }
                    consumedMessages.append(message)

                    if consumedMessages.count >= testMessages.count {
                        break
                    }
                }

                XCTAssertEqual(testMessages.count, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[index].topic, consumedMessage.topic)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].key!), consumedMessage.key)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].value), consumedMessage.value)
                }
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }
    class LoggerGuard {
        let logger: Logger
        let name: String
        
        public init(name: String = "Logger Guard", logger: Logger = Logger(label: "Logger Guard")) {
            self.logger = logger
            self.name = name
            
            logger.info("[\(name)] ctor")
        }
        
        public func void() {
            // to shut up warning about not used variable
        }
        
        deinit {
            logger.info("[\(self.name)] dtor")
        }
    }
    // TODO: copy to separate test and revert this one
    func testProduceAndConsumeWithAssignedTopicPartition() async throws {
        let testMessages = Self.createTestMessages(topic: "test-topic", count: 100)
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(config: self.producerConfig, logger: .kafkaTest)


        
        let consumerConfig = {
            var config = KafkaConsumerConfiguration(
                consumptionStrategy: .group(id: "test-consumers", topics: ["test-topic"]) //(
    //                KafkaPartition(rawValue: 0),
    //                topic: self.uniqueTestTopic,
    //                offset: 0
    //            )
            )
            config.debug = [.all]
            config.autoOffsetReset = .beginning // Always read topics from beginning
            config.bootstrapServers = [self.bootstrapServer]
            config.broker.addressFamily = .v4
            config.autoCommitIntervalMilliseconds = 10
            config.enableAutoCommit = false
            
            
            return config
        } ()

        let logger1 = {
            var logger = Logger(label: "Consumer1.Log")
            logger.logLevel = .debug
            return logger
        }()
        let logger2 = {
            var logger = Logger(label: "Consumer2.Log")
            logger.logLevel = logger1.logLevel
            return logger
        }()
        
        let (consumer, consumerEvents) = try KafkaConsumer.makeConsumerWithEvents(
            config: consumerConfig,
            logger: logger1
        )
        let (cons, consEvents) = try KafkaConsumer.makeConsumerWithEvents(
            config: consumerConfig,
            logger: logger2
        )

        let serviceGroup = ServiceGroup(
            services: [producer, /*consumer, */cons],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    events: events,
                    messages: testMessages
                )
            }

            // Consumer Task
            group.addTask {
                let g = LoggerGuard(name: "consumer 1", logger: logger1)
                var consumedMessages = [KafkaConsumerMessage]()
                for try await messageResult in consumer.messages {
                    if !consumerConfig.enableAutoCommit {
                        try await consumer.commitSync(messageResult)
                    }
                    guard case let message = messageResult else {
                        continue
                    }
                    consumedMessages.append(message)
                    if consumedMessages.count % max(testMessages.count / 10, 1) == 0 {
                        logger1.info("Got \(consumedMessages.count) out of \(testMessages.count)")
                    }

                    if consumedMessages.count >= testMessages.count {
                        break
                    }
                }

//                logger1.info("Task for consumer 1 finished, fetched \(consumedMessages.count)")
//                XCTAssertEqual(testMessages.count, consumedMessages.count)
//
//                for (index, consumedMessage) in consumedMessages.enumerated() {
//                    XCTAssertEqual(testMessages[index].topic, consumedMessage.topic)
//                    XCTAssertEqual(testMessages[index].key, consumedMessage.key)
//                    XCTAssertEqual(testMessages[index].value, consumedMessage.value)
//                }
            }
            
            group.addTask {
                let gg = LoggerGuard(name: "consumer 1 [events]", logger: logger1)
                for try await event in consumerEvents {
                    switch event {
                    case .rebalance(let action):
                        logger1.info("Rebalance action \(action)")
                        switch action {
                        case .assign(let proto, var topics):
                            logger2.info("Assign, proto [\(proto)], topics: \(topics)")
                            if case .cooperative = proto {
                                try cons.incrementalAssign(topics)
                            }
                            else {
                                try cons.assign(topics)
                            }
//                            topics.map { TopicPartition in
//
//                            }
//                            cons.
                            
                        case .revoke(let proto, let topics):
                            logger2.info("Revoke, proto [\(proto)], topics: \(topics)")
                            if case .cooperative = proto {
                                try cons.incrementalUnassign(topics)
//                                rd_kafka_incremental_unassign(handle, list)
                            } else {
                                try cons.assign(topics)
//                                rd_kafka_assign(handle, nil)
                            }
                        case .error(let proto, let topics, let err):
                            logger2.info("Error, proto [\(proto)], topics: \(topics), err: \(err)")
                        }
                    default:
                        break
                    }
//                    fatalError("Catched 2")
                }
            }

            group.addTask {
                logger2.info("Task for cons started")
                var aa = 0
                for try await messageResult in cons.messages {
                    try await cons.commitSync(messageResult)

                    aa += 1
                    if aa % max(testMessages.count / 10, 1) == 0 {
                        logger2.info("Got \(aa) out of \(testMessages.count)")
                    }
                    if aa >= testMessages.count {
                        break
                    }
                }
                logger2.info("Task for cons finished")
            }
            group.addTask {
                let gg = LoggerGuard(name: "consumer 1 [events]", logger: logger1)
                for try await event in consEvents {
                    switch event {
                    case .rebalance(let action):
                        logger2.info("Rebalance action \(action)")
                        switch action {
                        case .assign(let proto, var topics):
                            logger2.info("Assign, proto [\(proto)], topics: \(topics)")
                            if case .cooperative = proto {
                                try cons.incrementalAssign(topics)
                            }
                            else {
                                try cons.assign(topics)
                            }
//                            topics.map { TopicPartition in
//
//                            }
//                            cons.
                            
                        case .revoke(let proto, let topics):
                            logger2.info("Revoke, proto [\(proto)], topics: \(topics)")
                            if case .cooperative = proto {
                                try cons.incrementalUnassign(topics)
//                                rd_kafka_incremental_unassign(handle, list)
                            } else {
                                try cons.assign(topics)
//                                rd_kafka_assign(handle, nil)
                            }
                        case .error(let proto, let topics, let err):
                            logger2.info("Error, proto [\(proto)], topics: \(topics), err: \(err)")
                        }
                        
//                        print("Error: \(error) List: \(list.debugDescription)")
//                        fatalError("Catched 3")
                    default:
                        break
                    }
//                    fatalError("Catched 4")
                }
            }

            
//            group.addTask {
//                for try await msg in sequenceForAcks {
//
//                }
//            }

//            try? await Task.sleep(for: .seconds(5))

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testProduceAndConsumeWithCommitSync() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(config: self.producerConfig, logger: .kafkaTest)

        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "commit-sync-test-group-id", topics: [self.uniqueTestTopic])
        )
        consumerConfig.enableAutoCommit = false
        consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
        consumerConfig.bootstrapServers = [self.bootstrapServer]
        consumerConfig.broker.addressFamily = .v4

        let consumer = try KafkaConsumer(
            config: consumerConfig,
            logger: .kafkaTest
        )

        let serviceGroup = ServiceGroup(
            services: [producer, consumer],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Consumer Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    events: events,
                    messages: testMessages
                )
            }

            // Consumer Task
            group.addTask {
                var consumedMessages = [KafkaConsumerMessage]()
                for try await messageResult in consumer.messages {
                    guard case let message = messageResult else {
                        continue
                    }
                    consumedMessages.append(message)
                    try await consumer.commitSync(message)

                    if consumedMessages.count >= testMessages.count {
                        break
                    }
                }

                XCTAssertEqual(testMessages.count, consumedMessages.count)
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testCommittedOffsetsAreCorrect() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let firstConsumerOffset = testMessages.count / 2
        let (producer, acks) = try KafkaProducer.makeProducerWithEvents(config: self.producerConfig, logger: .kafkaTest)

        // Important: both consumer must have the same group.id
        let uniqueGroupID = UUID().uuidString

        // MARK: First Consumer

        var consumer1Config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                id: uniqueGroupID,
                topics: [self.uniqueTestTopic]
            )
        )
        consumer1Config.autoOffsetReset = .beginning // Read topic from beginning
        consumer1Config.bootstrapServers = [self.bootstrapServer]
        consumer1Config.broker.addressFamily = .v4

        let consumer1 = try KafkaConsumer(
            config: consumer1Config,
            logger: .kafkaTest
        )

        let serviceGroup1 = ServiceGroup(
            services: [producer, consumer1],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup1.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    events: acks,
                    messages: testMessages
                )
            }

            // First Consumer Task
            group.addTask {
                var consumedMessages = [KafkaConsumerMessage]()
                for try await messageResult in consumer1.messages {
                    guard case let message = messageResult else {
                        continue
                    }
                    consumedMessages.append(message)

                    // Only read first half of messages
                    if consumedMessages.count >= firstConsumerOffset {
                        break
                    }
                }

                XCTAssertEqual(firstConsumerOffset, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[index].topic, consumedMessage.topic)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].key!), consumedMessage.key)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].value), consumedMessage.value)
                }
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            // Wait for a couple of more run loop iterations.
            // We do this to process the remaining 5 messages.
            // These messages shall be discarded and their offsets should not be committed.
            try await Task.sleep(for: .seconds(2))
            // Shutdown the serviceGroup
            await serviceGroup1.triggerGracefulShutdown()
        }

        // MARK: Second Consumer

        // The first consumer has now read the first half of the messages in the test topic.
        // This means our second consumer should be able to read the second
        // half of messages without any problems.

        var consumer2Config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                id: uniqueGroupID,
                topics: [self.uniqueTestTopic]
            )
        )
        consumer2Config.autoOffsetReset = .largest
        consumer2Config.bootstrapServers = [self.bootstrapServer]
        consumer2Config.broker.addressFamily = .v4

        let consumer2 = try KafkaConsumer(
            config: consumer2Config,
            logger: .kafkaTest
        )

        let serviceGroup2 = ServiceGroup(
            services: [consumer2],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup2.run()
            }

            // Second Consumer Task
            group.addTask {
                var consumedMessages = [KafkaConsumerMessage]()
                for try await messageResult in consumer2.messages {
                    guard case let message = messageResult else {
                        continue
                    }
                    consumedMessages.append(message)

                    // Read second half of messages
                    if consumedMessages.count >= (testMessages.count - firstConsumerOffset) {
                        break
                    }
                }

                XCTAssertEqual(testMessages.count - firstConsumerOffset, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[firstConsumerOffset + index].topic, consumedMessage.topic)
                    XCTAssertEqual(ByteBuffer(string: testMessages[firstConsumerOffset + index].key!), consumedMessage.key)
                    XCTAssertEqual(ByteBuffer(string: testMessages[firstConsumerOffset + index].value), consumedMessage.value)
                }
            }

            // Wait for second Consumer Task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup2.triggerGracefulShutdown()
        }
    }

    // MARK: - Helpers

    private static func createTestMessages(topic: String, count: UInt) -> [KafkaProducerMessage<String, String>] {
        return Array(0..<count).map {
            KafkaProducerMessage(
                topic: topic,
                key: "key \($0)",
                value: "Hello, World! \($0) - \(Date().description)"
            )
        }
    }

    private static func sendAndAcknowledgeMessages(
        producer: KafkaProducer,
        events: KafkaProducerEvents,
        messages: [KafkaProducerMessage<String, String>]
    ) async throws {
        var messageIDs = Set<KafkaProducerMessageID>()

        for message in messages {
            messageIDs.insert(try producer.send(message))
        }

        var receivedDeliveryReports = Set<KafkaDeliveryReport>()

        for await event in events {
            switch event {
            case .deliveryReports(let deliveryReports):
                for deliveryReport in deliveryReports {
                    receivedDeliveryReports.insert(deliveryReport)
                }
            default:
                break // Ignore any other events
            }
            
            print("Sent \(receivedDeliveryReports.count) out of \(messages.count)")

            if receivedDeliveryReports.count >= messages.count {
                break
            }
        }

        XCTAssertEqual(Set(receivedDeliveryReports.map(\.id)), messageIDs)

        let acknowledgedMessages: [KafkaAcknowledgedMessage] = receivedDeliveryReports.compactMap {
            guard case .acknowledged(let receivedMessage) = $0.status else {
                return nil
            }
            return receivedMessage
        }

        XCTAssertEqual(messages.count, acknowledgedMessages.count)
        for message in messages {
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.topic == message.topic }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.key == ByteBuffer(string: message.key!) }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.value == ByteBuffer(string: message.value) }))
        }
    }
}

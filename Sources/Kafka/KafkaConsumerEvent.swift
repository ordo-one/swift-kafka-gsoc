//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka
import NIOConcurrencyHelpers

public struct KafkaTopicList {
    let list: RDKafkaTopicPartitionList
    
    init(from: RDKafkaTopicPartitionList) {
        self.list = from
    }
    
    public init(size: Int32 = 1) {
        self.list = RDKafkaTopicPartitionList(size: size)
    }
    
    public func append(topic: TopicPartition) {
        self.list.setOffset(topic: topic.topic, partition: topic.partition, offset: topic.offset)
    }
}

public struct TopicPartition {
    public let topic: String
    public let partition: KafkaPartition
    public let offset: KafkaOffset
    
    public init(_ topic: String, _ partition: KafkaPartition, _ offset: KafkaOffset) {
        self.topic = topic
        self.partition = partition
        self.offset = offset
    }
}

extension TopicPartition: Sendable {}
extension TopicPartition: Hashable {}

extension KafkaTopicList : Sendable {}
extension KafkaTopicList : Hashable {}

//extension KafkaTopicList : CustomDebugStringConvertible {
//    public var debugDescription: String {
//        list.debugDescription
//    }
//}

extension KafkaTopicList : Sequence {
    public struct TopicPartitionIterator : IteratorProtocol {
        private let list: RDKafkaTopicPartitionList
        private var idx = 0
        
        init(list: RDKafkaTopicPartitionList) {
            self.list = list
        }
        
        mutating public func next() -> TopicPartition? {
            guard let topic = list.getByIdx(idx: idx) else {
                return nil
            }
            idx += 1
            return topic
        }
    }
    
    public func makeIterator() -> TopicPartitionIterator {
        TopicPartitionIterator(list: self.list)
    }
}

public enum KafkaRebalanceProtocol: Sendable, Hashable {
    case cooperative
    case eager
    case none
    
    static func convert(from proto: String) -> KafkaRebalanceProtocol{
        switch proto {
        case "COOPERATIVE": return .cooperative
        case "EAGER": return .eager
        default: return .none
        }
    }
}


public enum RebalanceAction : Sendable, Hashable {
    case assign(KafkaRebalanceProtocol, KafkaTopicList)
    case revoke(KafkaRebalanceProtocol, KafkaTopicList)
    case error(KafkaRebalanceProtocol, KafkaTopicList, KafkaError)
}

final public class Rebalance: Sendable, CustomStringConvertible {
    private let client: RDKafkaClient
    let  rebalanceApplied: NIOLockedValueBox<Bool> = .init(false)


    public enum RebalanceProtocol: Sendable {
        case cooperative
        case eager
        case none

        static func convert(from proto: String) -> Self {
            switch proto {
            case "COOPERATIVE": return .cooperative
            case "EAGER": return .eager
            default: return .none
            }
        }
    }

    public enum RebalanceAction: Sendable {
        case assign(KafkaTopicList)
        case revoke(KafkaTopicList)
        case error(KafkaTopicList, KafkaError)
    }

    init(client: RDKafkaClient, rebalanceProtocol: RebalanceProtocol, rebalanceAction: RebalanceAction) {
        self.client = client
        self.rebalanceProtocol = rebalanceProtocol
        self.rebalanceAction = rebalanceAction

        client.logger.info("init rebalance")
    }

    deinit {
        if !rebalanceApplied.withLockedValue { $0 } {
            client.logger.info("Attention: rebalance auto triggered!")
            client.withKafkaHandlePointer {
                rd_kafka_assign($0, nil)
            }
        }
        client.logger.info("deinit rebalance")
    }

    public let rebalanceProtocol: RebalanceProtocol
    public let rebalanceAction: RebalanceAction

    public func assign(to partitions: KafkaTopicList?) async throws {
        try await client.assign(topicPartitionList: partitions?.list)
        applied()
    }

    public func assignIncremental(to partitions: KafkaTopicList) async throws {
        try await client.incrementalAssign(topicPartitionList: partitions.list)
        applied()
    }

    public func unassignIncremental(to partitions: KafkaTopicList) async throws {
        try await client.incrementalUnassign(topicPartitionList: partitions.list)
        applied()
    }

    public func seek(to partitions: KafkaTopicList, timeout: Duration = .seconds(1)) async throws {
        try await client.seek(topicPartitionList: partitions.list, timeout: timeout)
    }

    public func applied(_ funcName: Int = #line) {
        rebalanceApplied.withLockedValue {
            $0 = true
        }
//        rebalanceApplied = true
        client.logger.info("Rebalance applied \(funcName)")
    }

    public func apply() async throws {
        switch self.rebalanceAction {
        case .assign(let list):
            switch rebalanceProtocol {
            case .cooperative: try await assignIncremental(to: list)
            case .eager: try await assign(to: list)
            default: try await assign(to: nil)
            }
        case .revoke(let list):
            switch rebalanceProtocol {
            case .cooperative: try await unassignIncremental(to: list)
            case .eager: try await assign(to: nil)
            default: try await assign(to: nil)
            }
        default:
            try await assign(to: nil)
        }
    }

    public var description: String {
        "Rebalance: \(rebalanceProtocol): \(rebalanceAction)"
    }
}

/// An enumeration representing events that can be received through the ``KafkaConsumerEvents`` asynchronous sequence.
public enum KafkaConsumerEvent: Sendable {
    /// Rebalance from librdkafka
    case rebalance(Rebalance)
    /// - Important: Always provide a `default` case when switiching over this `enum`.
    case DO_NOT_SWITCH_OVER_THIS_EXHAUSITVELY

    internal init(_ event: RDKafkaClient.KafkaEvent) {
        switch event {
        case .statistics:
            fatalError("Cannot cast \(event) to KafkaConsumerEvent")
        case .rebalance(let action):
            self = .rebalance(action)
        case .deliveryReport:
            fatalError("Cannot cast \(event) to KafkaConsumerEvent")
        }
    }
}

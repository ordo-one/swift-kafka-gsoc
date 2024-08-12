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

//import Benchmark
import Crdkafka
import Dispatch
import struct Foundation.Date
import struct Foundation.UUID
import Kafka
import Logging
import ServiceLifecycle
import NIOConcurrencyHelpers
//let benchmarks = {
    var uniqueTestTopic: String!
    let messageCount: UInt = 1000

//    Benchmark.defaultConfiguration = .init(
//        metrics: [
//            .wallClock,
//            .cpuTotal,
//            .contextSwitches,
//            .throughput,
//            .allocatedResidentMemory,
//        ] + .arc,
//        warmupIterations: 0,
//        scalingFactor: .one,
//        maxDuration: .seconds(5),
//        maxIterations: 100,
//        thresholds: [
//            .wallClock: .init(relative: [.p90: 35]),
//            .cpuTotal: .init(relative: [.p90: 35]),
//            .allocatedResidentMemory: .init(relative: [.p90: 20]),
//            .contextSwitches: .init(relative: [.p90: 35]),
//            .throughput: .init(relative: [.p90: 35]),
//            .objectAllocCount: .init(relative: [.p90: 20]),
//            .retainCount: .init(relative: [.p90: 20]),
//            .releaseCount: .init(relative: [.p90: 20]),
//            .retainReleaseDelta: .init(relative: [.p90: 20]),
//        ]
//    )
//
//    Benchmark.setup = {
    //Benchmark("librdkafka_with_offset_commit_messages_\(messageCount)") { benchmark in

    func mainF() async throws {
        let uniqueTestTopic: String! = try await prepareTopic(messagesCount: messageCount, partitions: 6)
        let uniqueGroupID = UUID().uuidString
        let rdKafkaConsumerConfig: [String: String] = [
            "group.id": uniqueGroupID,
            "bootstrap.servers": "\(brokerAddress.host):\(brokerAddress.port)",
            "broker.address.family": "v4",
            "auto.offset.reset": "beginning",
            "enable.auto.commit": "false",
            "partition.assignment.strategy": "cooperative-sticky",
        ]

        let configPointer: OpaquePointer = rd_kafka_conf_new()
        for (key, value) in rdKafkaConsumerConfig {
            precondition(rd_kafka_conf_set(configPointer, key, value, nil, 0) == RD_KAFKA_CONF_OK)
        }

        enum Rebalance {
            case assign(UnsafeMutablePointer<rd_kafka_topic_partition_list_t>)
            case revoke(UnsafeMutablePointer<rd_kafka_topic_partition_list_t>)
        }

        final class ContinuationStore {
            let continuation: AsyncStream<Rebalance>.Continuation

            init(continuation: AsyncStream<Rebalance>.Continuation) {
                self.continuation = continuation
            }
        }
        let (stream, continuation) = AsyncStream<Rebalance>.makeStream()

        let store = ContinuationStore(continuation: continuation)
        let ptr = Unmanaged.passUnretained(store)


        rd_kafka_conf_set_opaque(configPointer, ptr.toOpaque())
        rd_kafka_conf_set_rebalance_cb(configPointer) { handle, code, topicPartition, opaque in
            print("Rebalance received, code: \(code), \(toString(topicPartition))")
            let rebalanceCont = Unmanaged<ContinuationStore>.fromOpaque(opaque!).takeUnretainedValue()

            switch code {
            case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                 rebalanceCont.continuation.yield(.assign(rd_kafka_topic_partition_list_copy(topicPartition)!))
            //    rd_kafka_incremental_assign(handle, topicPartition) 
            case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                 rebalanceCont.continuation.yield(.revoke(rd_kafka_topic_partition_list_copy(topicPartition)!))
            //    rd_kafka_incremental_unassign(handle, topicPartition) 
            default:
                fatalError("unexpected")
            }
        }

        let kafkaHandle = rd_kafka_new(RD_KAFKA_CONSUMER, configPointer, nil, 0)
        guard let kafkaHandle else {
            preconditionFailure("Kafka handle was not created")
        }
 //       defer {
//            print("Call kafka handle destroy")
//            rd_kafka_poll(kafkaHandle, 0)
//            rd_kafka_destroy(kafkaHandle)
//        }
        
        let rebalanceTask = Task {
            print("Rebalance task start")
            defer { print("Rebalance task exit") }
            for await rebalanceAction in stream {
                switch rebalanceAction {
                case .assign(let topicPartition):
                    print("assign \(toString(topicPartition))")
                    rd_kafka_incremental_assign(kafkaHandle, topicPartition)
                    rd_kafka_topic_partition_list_destroy(topicPartition)
                case .revoke(let topicPartition):
                    print("revoke \(toString(topicPartition))")
                    rd_kafka_incremental_unassign(kafkaHandle, topicPartition)
                    rd_kafka_topic_partition_list_destroy(topicPartition)
                }
            }
        }


        rd_kafka_poll_set_consumer(kafkaHandle)
        let subscriptionList = rd_kafka_topic_partition_list_new(1)
        defer {
            rd_kafka_topic_partition_list_destroy(subscriptionList)
        }
        rd_kafka_topic_partition_list_add(
            subscriptionList,
            uniqueTestTopic,
            RD_KAFKA_PARTITION_UA
        )
        rd_kafka_subscribe(kafkaHandle, subscriptionList)
        rd_kafka_poll(kafkaHandle, 0)

        var ctr: UInt64 = 0
        var tmpCtr: UInt64 = 0

        let interval: UInt64 = Swift.max(UInt64(messageCount / 20), 1)
        let totalStartDate = Date.timeIntervalSinceReferenceDate
        var totalBytes: UInt64 = 0

     //   benchmark.withMeasurement {
            while ctr < messageCount {
                guard let record = rd_kafka_consumer_poll(kafkaHandle, 10) else {
                    continue
                }
                defer {
                    rd_kafka_message_destroy(record)
                }
                guard record.pointee.err != RD_KAFKA_RESP_ERR__PARTITION_EOF else {
                    continue
                }
                let result = rd_kafka_commit_message(kafkaHandle, record, 0)
                precondition(result == RD_KAFKA_RESP_ERR_NO_ERROR)

                ctr += 1
                totalBytes += UInt64(record.pointee.len)

                tmpCtr += 1
                if tmpCtr >= interval {
                    benchLog("read \(ctr * 100 / UInt64(messageCount))%")
                    tmpCtr = 0
                }
            }
       // }

        let closingConsumer = Task {
            benchLog("Closing consumer")
            rd_kafka_consumer_close(kafkaHandle)
            benchLog("Consumer closed")
        }


        //await closingConsumer.value
        Task {
            benchLog("Deleting topic \(uniqueTestTopic)")
            if let uniqueTestTopic {
                try deleteTopic(uniqueTestTopic)
            }
            benchLog("Topic \(uniqueTestTopic) deleted")
        }

        benchLog("Finishing rebalance task")
        continuation.finish()
        await rebalanceTask.value


        benchLog("Destroying kafka handle")
        rd_kafka_destroy(kafkaHandle)
        benchLog("Destroyed")

        let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
        let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
        benchLog("All read up to ctr: \(ctr), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec")
    }

func toString(_ topicPartition: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>?) -> String {
    guard let topicPartition = topicPartition?.pointee else { return "<nil>" }
    return Array(0..<Int(topicPartition.cnt)).map {
        "\(String(cString: topicPartition.elems[$0].topic)) -> \(topicPartition.elems[$0].partition) [\(topicPartition.elems[$0].offset)]" 
    }.joined(separator: "\n")
}

try await mainF()

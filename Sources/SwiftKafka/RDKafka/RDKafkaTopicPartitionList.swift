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

import Crdkafka

/// Swift wrapper type for `rd_kafka_topic_partition_list_t`.
public final class RDKafkaTopicPartitionList {
    private let _internal: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>

    /// Create a new topic+partition list.
    ///
    /// - Parameter size: Initial allocated size used when the number of allocated elements can be estimated.
    init(size: Int32 = 1) {
        self._internal = rd_kafka_topic_partition_list_new(size)
    }

    /// for rebalance
    init(from unownedList: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>) {
        self._internal = rd_kafka_topic_partition_list_copy(unownedList)
    }

    deinit {
        rd_kafka_topic_partition_list_destroy(self._internal)
    }

    /// Add topic+partition pair to list.
    func add(topic: String, partition: KafkaPartition) {
        rd_kafka_topic_partition_list_add(
            self._internal,
            topic,
            partition.rawValue
        )
    }

    /// Manually set read offset for a given topic+partition pair.
    func setOffset(topic: String, partition: KafkaPartition, offset: Int64) {
        guard let partitionPointer = rd_kafka_topic_partition_list_add(
            self._internal,
            topic,
            partition.rawValue
        ) else {
            fatalError("rd_kafka_topic_partition_list_add returned invalid pointer")
        }
        partitionPointer.pointee.offset = offset
    }

    /// Scoped accessor that enables safe access to the pointer of the underlying `rd_kafka_topic_partition_t`.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the pointer.
    @discardableResult
    func withListPointer<T>(_ body: (UnsafeMutablePointer<rd_kafka_topic_partition_list_t>) throws -> T) rethrows -> T {
        return try body(self._internal)
    }

    /// Scoped accessor that enables safe access to the pointer of the underlying `rd_kafka_topic_partition_t`.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the pointer.
    @discardableResult
    func withListPointer<T>(_ body: (UnsafeMutablePointer<rd_kafka_topic_partition_list_t>) async throws -> T) async rethrows -> T {
        return try await body(self._internal)
    }
    
    func sorted() -> RDKafkaTopicPartitionList {
        let partitions = rd_kafka_topic_partition_list_copy(self._internal)
        guard let partitions else {
            fatalError("Partitions were not copied")
        }
        rd_kafka_topic_partition_list_sort(partitions, nil, nil)
        return RDKafkaTopicPartitionList(from: partitions)
    }
    
    // NB! return unowned
    func getByIdx(idx: Int) -> RDKafkaTopicPartition? {
        guard idx < Int(self._internal.pointee.cnt) else {
            return nil
        }
        return RDKafkaTopicPartition(self, self._internal.pointee.elems[idx])
    }
}

extension RDKafkaTopicPartitionList : Sendable {}
extension RDKafkaTopicPartitionList : Hashable {
    /*
     typedef struct rd_kafka_topic_partition_s {
             char *topic;             /**< Topic name */
             int32_t partition;       /**< Partition */
             int64_t offset;          /**< Offset */
             void *metadata;          /**< Metadata */
             size_t metadata_size;    /**< Metadata size */
             void *opaque;            /**< Opaque value for application use */
             rd_kafka_resp_err_t err; /**< Error code, depending on use. */
     */

    public func hash(into hasher: inout Hasher) {
        for idx in 0..<Int(self._internal.pointee.cnt) {
            // FIXME: probably we should only take hash from `topic/offset/partition`?
            let elem = self._internal.pointee.elems[idx]
            hasher.combine(String(cString: elem.topic))
            hasher.combine(elem.partition)
            hasher.combine(elem.offset)
            hasher.combine(elem.metadata_size)
            if elem.metadata_size != 0 {
                hasher.combine(bytes: UnsafeRawBufferPointer(start: elem.metadata, count: elem.metadata_size))
            }
            hasher.combine(elem.opaque)
            hasher.combine(elem.err.rawValue)
        }
    }
    
    public static func == (lhs: RDKafkaTopicPartitionList, rhs: RDKafkaTopicPartitionList) -> Bool {
        guard lhs._internal.pointee.cnt == rhs._internal.pointee.cnt else {
            return false
        }
        let sortedLhs = lhs.sorted()
        let sortedRhs = lhs.sorted()
        let lhsPtr = sortedLhs._internal.pointee
        let rhsPtr = sortedRhs._internal.pointee
        let size = Int(sortedLhs._internal.pointee.cnt)
        for idx in 0..<size {
            let lhsElem = lhsPtr.elems[idx]
            let rhsElem = rhsPtr.elems[idx]
            // FIXME: probably we should only compare `topic`?
            guard 0 == strcmp(lhsElem.topic, rhsElem.topic),
                  lhsElem.partition == rhsElem.partition,
                  lhsElem.offset == rhsElem.offset,
                  lhsElem.metadata_size == rhsElem.metadata_size,
                  0 == memcmp(lhsElem.metadata, rhsElem.metadata, lhsElem.metadata_size),
                  lhsElem.opaque == rhsElem.opaque,
                  lhsElem.err == rhsElem.err
            else {
                return false
            }
        }
        return true
    }
}

extension RDKafkaTopicPartitionList : CustomDebugStringConvertible {
    public var debugDescription: String {
        let size = Int(self._internal.pointee.cnt)
        var descr = "KafkaTopicPartitionList [\(size)]" // FIXME: can we .map { } it?
        for idx in 0..<size {
            let elem = self._internal.pointee.elems[idx]
            descr += "\n"
            descr +=
            """
            [\(idx)] ->
                Topic: \(String(cString: elem.topic))
                Partition: \(elem.partition)
                Offset: \(elem.offset)
            """
        }
        return descr
    }
}

// Strange thing is:
// Method 'makeIterator()' must be declared public because it matches a requirement in public protocol 'Sequence'
// or: Method cannot be declared public because its result uses an internal type
//extension RDKafkaTopicPartitionList: Sequence {
//    struct RDKafkaTopicPartitionIterator : IteratorProtocol {
//        private var list: RDKafkaTopicPartitionList?
//        private var idx = 0
//
//        init(list: RDKafkaTopicPartitionList) {
//            self.list = list
//        }
//
//        mutating func next() -> RDKafkaTopicPartition? {
//            guard let topic = list?.getByIdx(idx: idx) else {
//                list = nil
//                return nil
//            }
//            idx += 1
//            return topic
//        }
//    }
//
//    public func makeIterator() -> RDKafkaTopicPartitionIterator {
//        RDKafkaTopicPartitionIterator(list: self)
//    }
//}

struct RDKafkaTopicPartition {
    // save list to avoid destruction of unowned `topicPartition`
    private let list: RDKafkaTopicPartitionList
    let topicPartition: rd_kafka_topic_partition_t
    
    init(_ list: RDKafkaTopicPartitionList, _ topicPartition: rd_kafka_topic_partition_t) {
        self.list = list
        self.topicPartition = topicPartition
    }

    var topic: String {
        String(cString: topicPartition.topic)
    }
    
    var partition: KafkaPartition {
        KafkaPartition(rawValue: topicPartition.partition)
    }
    
    var offset: Int64 {
        topicPartition.offset
    }
}

public struct RDKafkaOffset: RawRepresentable, Hashable, Sendable {
    public typealias RawValue = Int64
    public var rawValue: RawValue

    public init(rawValue: RawValue) {
        self.rawValue = rawValue
    }

    public static let beginning = Self(rawValue: RawValue(RD_KAFKA_OFFSET_BEGINNING))
    public static let end = Self(rawValue: RawValue(RD_KAFKA_OFFSET_END))
    public static let stored = Self(rawValue: RawValue(RD_KAFKA_OFFSET_STORED))
    public static let invalid = Self(rawValue: RawValue(RD_KAFKA_OFFSET_INVALID))

    public static let min = Self(rawValue: RawValue.min)
    public static let zero = Self(rawValue: 0)
}

extension RDKafkaOffset: Comparable {
    public static func < (lhs: RDKafkaOffset, rhs: RDKafkaOffset) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

import Crdkafka

public struct KafkaMetadata: Sendable {
    public let topics: [KafkaTopicMetadata]

    init(metadata: borrowing UnsafePointer<rd_kafka_metadata>) {
        /*
         cannot use the following with the following error:
         'metadata' cannot be captured by an escaping closure since it is a borrowed parameter
         self.topics = (0..<Int(metadata.pointee.topic_cnt)).map { KafkaTopicMetadata(topic: metadata.pointee.topics[$0]) }
         */

        var topics: [KafkaTopicMetadata] = .init()
        topics.reserveCapacity(Int(metadata.pointee.topic_cnt))
        for i in 0..<Int(metadata.pointee.topic_cnt) {
            topics.append(KafkaTopicMetadata(topic: metadata.pointee.topics[i]))
        }
        self.topics = topics
    }
}

public struct KafkaTopicMetadata: Sendable {
    public let name: String
    public let partitions: [KafkaPartitionMetadata]

    init(topic: rd_kafka_metadata_topic) {
        self.name = String(cString: topic.topic)
        self.partitions = (0..<Int(topic.partition_cnt)).map { KafkaPartitionMetadata( topic.partitions[$0]) }
    }
}

public struct KafkaPartitionMetadata: Sendable {
    public let id: Int
    public let replicasCount: Int
    
    init(_ partition: rd_kafka_metadata_partition) {
        self.id = Int(partition.id)
        self.replicasCount = Int(partition.replica_cnt)
    }
}

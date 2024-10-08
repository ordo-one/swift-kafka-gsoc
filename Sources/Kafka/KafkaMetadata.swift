import Crdkafka

public struct KafkaMetadata: Sendable {
    public let topics: [KafkaTopicMetadata]

    init(metadata: UnsafePointer<rd_kafka_metadata> /* unowned */) {
        self.topics = (0..<Int(metadata.pointee.topic_cnt)).map {
            KafkaTopicMetadata(topic: metadata.pointee.topics[$0])
        }
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

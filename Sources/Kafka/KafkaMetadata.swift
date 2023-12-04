import Crdkafka

public final class KafkaMetadata {
    private let metadata: UnsafePointer<rd_kafka_metadata>
    
    init(metadata: UnsafePointer<rd_kafka_metadata>) {
        self.metadata = metadata
    }
    
    deinit {
        rd_kafka_metadata_destroy(metadata)
    }
    
    public var topics: [KafkaMetadataTopic] {
        let metadata = self.metadata.pointee
        let topicCount = Int(metadata.topic_cnt)
        var topics = [KafkaMetadataTopic]()
        topics.reserveCapacity(topicCount)
        
        for i in 0..<topicCount {
            topics.append(.init(metadata: self, topic: metadata.topics[i]))
        }
        return topics
    }
}

public struct KafkaMetadataTopic {
    private let metadata: KafkaMetadata // retain metadata
    private let topic: rd_kafka_metadata_topic
    
    init(metadata: KafkaMetadata, topic: rd_kafka_metadata_topic) {
        self.metadata = metadata
        self.topic = topic
    }
    
    public var name: String {
        String(cString: topic.topic)
    }
    
    public var partitions: [KafkaMetadataPartition] {
        let partitionCount = Int(topic.partition_cnt)
        
        var partitions = [KafkaMetadataPartition]()
        partitions.reserveCapacity(partitionCount)
        
        for i in 0..<partitionCount {
            partitions.append(.init(metadata: metadata, partition: topic.partitions[i]))
        }
        
        return partitions
    }
}

public struct KafkaMetadataPartition {
    private let metadata: KafkaMetadata // retain metadata
    private let partition: rd_kafka_metadata_partition
    
    init(metadata: KafkaMetadata, partition: rd_kafka_metadata_partition) {
        self.metadata = metadata
        self.partition = partition
    }
    
    var id: Int {
        Int(partition.id)
    }

    var replicasCount: Int {
        Int(partition.replica_cnt)
    }
}

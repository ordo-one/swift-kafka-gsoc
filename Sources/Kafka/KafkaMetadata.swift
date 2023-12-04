import Crdkafka

public final class KafkaMetadata {
    private let metadata: UnsafePointer<rd_kafka_metadata>
    
    init(metadata: UnsafePointer<rd_kafka_metadata>) {
        self.metadata = metadata
    }
    
    deinit {
        rd_kafka_metadata_destroy(metadata)
    }
    
    public private(set) lazy var topics = {
        let metadata = self.metadata.pointee
        let topicCount = Int(metadata.topic_cnt)
        var topics = [KafkaMetadataTopic]()
        topics.reserveCapacity(topicCount)
        
        for i in 0..<topicCount {
            topics.append(.init(metadata: self, topic: metadata.topics[i]))
        }
        return topics
    }()
}

// must be a class to allow mutating lazy vars, otherwise require struct copies
public final class KafkaMetadataTopic {
    private let metadata: KafkaMetadata // retain metadata
    private let topic: rd_kafka_metadata_topic
    
    init(metadata: KafkaMetadata, topic: rd_kafka_metadata_topic) {
        self.metadata = metadata
        self.topic = topic
    }

    public private(set) lazy var name = {
        String(cString: self.topic.topic)
    }()
    
    public private(set) lazy var partitions = {
        let partitionCount = Int(self.topic.partition_cnt)
        
        var partitions = [KafkaMetadataPartition]()
        partitions.reserveCapacity(partitionCount)
        
        for i in 0..<partitionCount {
            partitions.append(.init(metadata: self.metadata, partition: topic.partitions[i]))
        }
        
        return partitions
    }()
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

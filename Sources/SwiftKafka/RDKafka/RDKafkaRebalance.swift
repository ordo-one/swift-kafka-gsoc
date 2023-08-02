import Crdkafka

enum RDKafkaRebalanceProtocol: Sendable, Hashable {
    case cooperative
    case eager
    case none
    
    static func rebalanceProtocol(from str: String) -> RDKafkaRebalanceProtocol {
        switch str {
        case "COOPERATIVE": return .cooperative
        case "EAGER": return .eager
        default: return .none
        }
    }
}

enum RDKafkaRebalanceAction: Sendable, Hashable {
    case assign(RDKafkaRebalanceProtocol, RDKafkaTopicPartitionList)
    case revoke(RDKafkaRebalanceProtocol, RDKafkaTopicPartitionList)
    case error(RDKafkaRebalanceProtocol, RDKafkaTopicPartitionList, KafkaError)
}


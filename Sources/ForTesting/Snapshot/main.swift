import Kafka
import Foundation
import NIOCore
import ServiceLifecycle
import Logging

enum ExitErr: Error {
    case exit
}

func sendAndAcknowledgeMessages(
    producer: KafkaProducer,
    events: KafkaProducerEvents,
    messages: [KafkaProducerMessage<String, String>]
) async throws {
    for message in messages {
        while true { // Note: this is an example of queue full
            do {
                try producer.send(message)
                break
            } catch let error as KafkaError where error.description.contains("Queue full") {
                continue
            } catch {
                print("Caught some error: \(error)")
                throw error
            }
        }
    }

    var receivedDeliveryReportsCtr = 0
    var prevPercent = 0

    for await event in events {
        switch event {
        case .deliveryReports(let deliveryReports):
            receivedDeliveryReportsCtr += deliveryReports.count
        default:
            break // Ignore any other events
        }
        let curPercent = receivedDeliveryReportsCtr * 100 / messages.count
        if curPercent >= prevPercent + 10 {
            print("Delivered \(curPercent)% of messages")
            prevPercent = curPercent
        }

        if receivedDeliveryReportsCtr >= messages.count {
            break
        }
    }

}

func createTestMessages(
    topic: String,
    headers: [KafkaHeader] = [],
    count: UInt
) -> [KafkaProducerMessage<String, String>] {
    return Array(0..<count).map {
        KafkaProducerMessage(
            topic: topic,
            headers: headers,
            key: "key \($0)",
            value: "Hello, World! \($0) - \(Date().description)"
        )
    }
}

var logger = Logger(label: "snapshot")
logger.logLevel = .info // .debug
let kafkaHost: String = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
let kafkaPort: Int = .init(ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092")!
let numOfMessages: UInt = .init(ProcessInfo.processInfo.environment["MESSAGES_NUMBER"] ?? "15000000")!
var bootstrapBrokerAddress: KafkaConfiguration.BrokerAddress!
var producerConfig: KafkaProducerConfiguration!
var uniqueTestTopic: String!

bootstrapBrokerAddress = KafkaConfiguration.BrokerAddress(
    host: kafkaHost,
    port: kafkaPort
)

producerConfig = KafkaProducerConfiguration(bootstrapBrokerAddresses: [bootstrapBrokerAddress])
producerConfig.broker.addressFamily = .v4

var basicConfig = KafkaConsumerConfiguration(
    consumptionStrategy: .group(id: "no-group", topics: []),
    bootstrapBrokerAddresses: [bootstrapBrokerAddress]
)
basicConfig.broker.addressFamily = .v4

let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: basicConfig.dictionary,
            events: [],
            logger: logger
        )
uniqueTestTopic = try client._createUniqueTopic(timeout: 10 * 1000)

defer {
    try? client._deleteTopic(uniqueTestTopic, timeout: -1)
}
print("Generating \(numOfMessages) messages")
let testMessages = createTestMessages(topic: uniqueTestTopic, count: numOfMessages)
print("Finish generating \(numOfMessages) messages")
let (producer, acks) = try KafkaProducer.makeProducerWithEvents(configuration: producerConfig, logger: logger)


let serviceGroupConfiguration1 = ServiceGroupConfiguration(services: [producer], gracefulShutdownSignals: [.sigterm, .sigint], logger: logger)
let serviceGroup1 = ServiceGroup(configuration: serviceGroupConfiguration1)

try await withThrowingTaskGroup(of: Void.self) { group in
    print("Start producing \(numOfMessages) messages")
    defer {
        print("Finish producing")
    }
    // Run Task
    group.addTask {
        try await serviceGroup1.run()
    }

    // Producer Task
    group.addTask {
        try await sendAndAcknowledgeMessages(
            producer: producer,
            events: acks,
            messages: testMessages
        )
    }

    // Wait for Producer Task to complete
    try await group.next()
    // Wait for a couple of more run loop iterations.
    // We do this to process the remaining 5 messages.
    // These messages shall be discarded and their offsets should not be committed.
    //try await Task.sleep(for: .seconds(2))
    // Shutdown the serviceGroup
    await serviceGroup1.triggerGracefulShutdown()
}

do {
    print("current swift-kafka implementation")
    // MARK: Consumer

    // The first consumer has now read the first half of the messages in the test topic.
    // This means our second consumer should be able to read the second
    // half of messages without any problems.

    let uniqueGroupID = UUID().uuidString
    var consumer2Config = KafkaConsumerConfiguration(
        consumptionStrategy: .group(
            id: uniqueGroupID,
            topics: [uniqueTestTopic]
        ),
        bootstrapBrokerAddresses: [bootstrapBrokerAddress]
    )
    consumer2Config.autoOffsetReset = .beginning
    consumer2Config.broker.addressFamily = .v4
    consumer2Config.pollInterval = .milliseconds(1)

    let consumer2 = try KafkaConsumer(
        configuration: consumer2Config,
        logger: logger
    )

    let serviceGroupConfiguration2 = ServiceGroupConfiguration(services: [consumer2], gracefulShutdownSignals: [.sigterm, .sigint], logger: logger)
    let serviceGroup2 = ServiceGroup(configuration: serviceGroupConfiguration2)

    try await withThrowingTaskGroup(of: Void.self) { group in
        print("Start consuming")
        defer {
            print("Finish consuming")
        }
        // Run Task
        group.addTask {
            try await serviceGroup2.run()
        }

        // Second Consumer Task
        group.addTask {
            var ctr: UInt64 = 0
            var tmpCtr: UInt64 = 0
            let interval: UInt64 = Swift.max(UInt64(numOfMessages / 20), 1)
            let totalStartDate = Date.timeIntervalSinceReferenceDate
            var totalBytes: UInt64 = 0
            
            for try await record in consumer2.messages {
                ctr += 1
                totalBytes += UInt64(record.value.readableBytes)
            
                tmpCtr += 1
                if tmpCtr >= interval {
                    print("read \(ctr * 100 / UInt64(numOfMessages))%")
                    tmpCtr = 0
                }
                if ctr >= numOfMessages {
                    break
                }
            }
            let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
            let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
            print("All read up to ctr: \(ctr), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec")
        }

        // Wait for second Consumer Task to complete
        try await group.next()
        // Shutdown the serviceGroup
        await serviceGroup2.triggerGracefulShutdown()
    }
}

do {
    print("librdkafka pure msg")
    // MARK: Consumer

    // The first consumer has now read the first half of the messages in the test topic.
    // This means our second consumer should be able to read the second
    // half of messages without any problems.

    let uniqueGroupID = UUID().uuidString
    var consumer2Config = KafkaConsumerConfiguration(
        consumptionStrategy: .group(
            id: uniqueGroupID,
            topics: [uniqueTestTopic]
        ),
        bootstrapBrokerAddresses: [bootstrapBrokerAddress]
    )
    consumer2Config.autoOffsetReset = .beginning
    consumer2Config.broker.addressFamily = .v4
    consumer2Config.pollInterval = .milliseconds(1)

    let consumer2 = try KafkaConsumer(
        configuration: consumer2Config,
        logger: logger
    )
    
    var ctr: UInt64 = 0
    var tmpCtr: UInt64 = 0
    
    let interval: UInt64 = Swift.max(UInt64(numOfMessages / 20), 1)
    let totalStartDate = Date.timeIntervalSinceReferenceDate
    var totalBytes: UInt64 = 0

    try? await consumer2.runPure { record in
        ctr += 1
        totalBytes += UInt64(record.pointee.len)
        
        tmpCtr += 1
        if tmpCtr >= interval {
            print("read \(ctr * 100 / UInt64(numOfMessages))%")
            tmpCtr = 0
        }
        if ctr >= numOfMessages {
            let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
            let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
            print("All read up to ctr: \(ctr), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec")
            throw ExitErr.exit
        }
    }
}

# Kafka Interview Questions and Answers for 7+ Years Experience

This document contains comprehensive Kafka interview questions and answers suitable for candidates with 7+ years of experience. The questions cover basic concepts, advanced topics, Spring Boot integration, architecture, performance optimization, resilience patterns, and real-world scenarios.

## Table of Contents
1. [Basic Kafka Concepts](#basic-kafka-concepts)
2. [Advanced Kafka Topics](#advanced-kafka-topics)
3. [Kafka with Spring Boot](#kafka-with-spring-boot)
4. [Kafka Architecture and Performance](#kafka-architecture-and-performance)
5. [Kafka Resilience Patterns](#kafka-resilience-patterns)
6. [Real-World Scenarios and Best Practices](#real-world-scenarios-and-best-practices)

## Basic Kafka Concepts

### Q1: What is Apache Kafka and what are its key components?
**Answer:** Apache Kafka is a distributed streaming platform designed for building real-time data pipelines and streaming applications. It is horizontally scalable, fault-tolerant, and extremely fast.

Key components include:
- **Topics**: Categories or feed names to which records are published
- **Partitions**: Topics are divided into partitions for parallelism
- **Brokers**: Servers that store the data and serve client requests
- **Producer API**: Allows applications to send streams of data to Kafka topics
- **Consumer API**: Allows applications to read streams of data from Kafka topics
- **Streams API**: Allows transforming streams of data from input topics to output topics
- **Connect API**: Allows building and running reusable producers/consumers that connect topics to applications or data systems
- **ZooKeeper**: Used for managing and coordinating Kafka brokers (though Kafka is moving away from ZooKeeper dependency in newer versions)

### Q2: Explain the difference between a queue and Kafka. Why would you choose one over the other?
**Answer:** 
Traditional message queues and Kafka differ in several key aspects:

1. **Message Retention**:
   - Traditional queues: Messages are typically deleted after consumption
   - Kafka: Messages are retained for a configurable period regardless of consumption

2. **Consumption Model**:
   - Traditional queues: Usually follow a point-to-point model where a message is consumed by a single consumer
   - Kafka: Follows a publish-subscribe model where multiple consumer groups can read the same message

3. **Scalability**:
   - Traditional queues: Often have limitations on throughput and scaling
   - Kafka: Designed for high throughput with horizontal scaling capabilities

4. **Order Guarantee**:
   - Traditional queues: Often provide global ordering
   - Kafka: Provides ordering within a partition but not across partitions

Choose Kafka when:
- You need high throughput
- You need to retain messages for replay
- You have multiple consumers that need the same data
- You need horizontal scalability
- You're building event-sourcing or stream processing applications

Choose a traditional queue when:
- You need strict FIFO ordering across all messages
- You need exactly-once delivery semantics without additional work
- You have simple producer-consumer requirements without the need for Kafka's complexity
- You don't need message retention after consumption

### Q3: How does Kafka ensure fault tolerance and high availability?
**Answer:** Kafka ensures fault tolerance and high availability through several mechanisms:

1. **Replication**: Each partition can be replicated across multiple brokers. One broker acts as the leader for a partition, handling all reads and writes, while others serve as followers that replicate the data.

2. **Leader Election**: If a leader fails, one of the in-sync replicas (ISRs) is automatically elected as the new leader.

3. **Acknowledgments (acks)**: Producers can configure acknowledgment levels:
   - `acks=0`: No acknowledgment (fire and forget)
   - `acks=1`: Leader acknowledgment only
   - `acks=all`: Full acknowledgment from all in-sync replicas

4. **Distributed Architecture**: Kafka's distributed nature allows it to continue operating even if some nodes fail.

5. **Rack Awareness**: Kafka can distribute replicas across different racks or availability zones to protect against rack-level failures.

6. **Broker Redundancy**: Multiple brokers form a cluster, allowing the system to continue functioning even if some brokers fail.

7. **Data Rebalancing**: When brokers are added or removed, Kafka can rebalance partitions to maintain even distribution.

## Advanced Kafka Topics

### Q4: Explain Kafka's exactly-once semantics and how it's implemented.
**Answer:** Exactly-once semantics ensures that each record is processed exactly once, even in the face of failures. Kafka implemented true exactly-once semantics in version 0.11.0 with the introduction of the Transactions API.

The implementation involves several components:

1. **Producer Idempotence**: Producers are assigned a unique ID and sequence numbers for each partition. Brokers use these to detect and reject duplicates.

2. **Transactions API**: Allows atomic writes to multiple partitions and topics. This ensures that either all messages in a transaction are written or none are.

3. **Consumer Offsets**: Stored atomically with the transaction, ensuring that messages are not reprocessed if a consumer fails and restarts.

4. **Transaction Coordinator**: A specialized broker role that manages the state of transactions across the cluster.

To use exactly-once semantics:
- Set `enable.idempotence=true` in producer config
- Use the Transactions API with `producer.initTransactions()`, `producer.beginTransaction()`, `producer.commitTransaction()`, and `producer.abortTransaction()`
- Set `isolation.level=read_committed` in consumer config to read only committed messages

Exactly-once semantics comes with some performance overhead and is typically used when data accuracy is critical, such as in financial applications.

### Q5: Describe Kafka's log compaction feature and when you would use it.
**Answer:** Log compaction is a feature that ensures Kafka retains at least the last known value for each record key within the log for a single topic partition. It's an alternative to time-based or size-based retention.

How it works:
1. Kafka maintains a "clean" portion of the log that has been compacted and a "dirty" portion that is yet to be compacted.
2. The compaction process retains only the most recent message for each key in the dirty portion and discards older messages with the same key.
3. Tombstone messages (messages with null values) are retained for a configurable period before being removed.

Use cases for log compaction:
1. **Key-Value Stores**: When Kafka is used as a backing store for a key-value database
2. **Change Data Capture (CDC)**: When capturing database changes where only the latest state matters
3. **Configuration Management**: When storing application configurations where only the current config is relevant
4. **Event Sourcing with Snapshots**: When implementing event sourcing patterns with periodic state snapshots
5. **User Preference Storage**: When storing user preferences where only the latest preference matters

Configuration parameters:
- `cleanup.policy=compact` (or `cleanup.policy=compact,delete` for hybrid approach)
- `min.cleanable.dirty.ratio`: Controls how frequently compaction runs
- `delete.retention.ms`: How long tombstone messages are retained

### Q6: How would you handle schema evolution in Kafka?
**Answer:** Schema evolution in Kafka is typically handled using schema registries and serialization frameworks. The most common approach is to use Apache Avro with the Confluent Schema Registry.

Key strategies for schema evolution:

1. **Schema Registry**: A centralized service that stores and retrieves schemas. Producers register schemas when writing data, and consumers retrieve schemas when reading data.

2. **Compatibility Types**:
   - **Backward Compatibility**: New schema can read old data (can remove fields, add optional fields)
   - **Forward Compatibility**: Old schema can read new data (can add fields, remove optional fields)
   - **Full Compatibility**: Both backward and forward compatible
   - **Breaking Changes**: Require version management or topic recreation

3. **Schema IDs**: Each message includes a schema ID (not the full schema), which references the schema in the registry. This keeps message size small.

4. **Serialization Frameworks**:
   - **Avro**: Most commonly used with Kafka, supports rich schema evolution
   - **Protobuf**: Good for multi-language environments
   - **JSON Schema**: More human-readable but less efficient

5. **Version Management**: Using subject naming strategies like:
   - Topic name strategy: `<topic>-key` and `<topic>-value`
   - Record name strategy: Based on the record/schema name
   - Topic record name strategy: Combination of both

6. **Compatibility Enforcement**: Schema Registry can enforce compatibility rules, rejecting incompatible schema updates.

Best practices:
- Use meaningful field names that won't need to change
- Use default values for new fields to maintain backward compatibility
- Consider using nullable fields instead of required fields
- Test schema changes before deploying to production
- Document schema evolution policies for your organization

## Kafka with Spring Boot

### Q7: Explain how to implement a Kafka producer and consumer in Spring Boot.
**Answer:** Spring Boot provides excellent integration with Kafka through the `spring-kafka` library. Here's how to implement producers and consumers:

**1. Dependencies**:
```xml
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>
```

**2. Configuration**:
```java
@Configuration
public class KafkaConfig {
    
    // Producer configuration
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }
    
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    
    // Consumer configuration
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.model");
        return new DefaultKafkaConsumerFactory<>(configProps);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
    
    // Topic configuration
    @Bean
    public NewTopic myTopic() {
        return new NewTopic("my-topic", 3, (short) 1);
    }
}
```

**3. Producer Implementation**:
```java
@Service
public class KafkaProducerService {
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Autowired
    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    public void sendMessage(String topic, Object message) {
        kafkaTemplate.send(topic, message);
    }
    
    public void sendMessageWithKey(String topic, String key, Object message) {
        kafkaTemplate.send(topic, key, message);
    }
    
    // With callback
    public void sendMessageWithCallback(String topic, Object message) {
        ListenableFuture<SendResult<String, Object>> future = 
            kafkaTemplate.send(topic, message);
            
        future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onSuccess(SendResult<String, Object> result) {
                log.info("Message sent successfully: {}", result.getRecordMetadata());
            }
            
            @Override
            public void onFailure(Throwable ex) {
                log.error("Failed to send message", ex);
            }
        });
    }
}
```

**4. Consumer Implementation**:
```java
@Service
public class KafkaConsumerService {
    
    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void consume(Object message) {
        log.info("Received message: {}", message);
        // Process the message
    }
    
    // With specific type
    @KafkaListener(topics = "user-topic", groupId = "user-group")
    public void consumeUser(User user) {
        log.info("Received user: {}", user);
        // Process the user
    }
    
    // With manual acknowledgment
    @KafkaListener(topics = "critical-topic", groupId = "critical-group", 
                  containerFactory = "kafkaManualAckListenerContainerFactory")
    public void consumeWithManualAck(Object message, Acknowledgment ack) {
        try {
            log.info("Processing message: {}", message);
            // Process the message
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing message", e);
            // Handle error, possibly retry or send to DLQ
        }
    }
    
    // With concurrency
    @KafkaListener(topics = "high-volume-topic", groupId = "high-volume-group", 
                  concurrency = "3")
    public void consumeWithConcurrency(Object message) {
        log.info("Processing high-volume message: {}", message);
        // Process the message
    }
}
```

**5. Error Handling**:
```java
@Configuration
public class KafkaErrorHandlingConfig {
    
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        // Basic configuration as before
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Configure error handler
        factory.setErrorHandler(new SeekToCurrentErrorHandler(
            new DeadLetterPublishingRecoverer(kafkaTemplate), 
            new FixedBackOff(1000L, 3))); // Retry 3 times with 1s interval
            
        return factory;
    }
}
```

**6. Application Properties (alternative to Java config)**:
```properties
# Producer properties
spring.kafka.producer.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer

# Consumer properties
spring.kafka.consumer.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=my-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=com.example.model

# Listener properties
spring.kafka.listener.missing-topics-fatal=false
spring.kafka.listener.ack-mode=MANUAL_IMMEDIATE
```

### Q8: How would you implement retry logic and dead letter queues with Spring Kafka?
**Answer:** Implementing retry logic and dead letter queues (DLQs) in Spring Kafka involves configuring error handlers and recovery strategies. Here's a comprehensive approach:

**1. Basic Retry Configuration**:
```java
@Configuration
public class KafkaRetryConfig {
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate) {
            
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        
        // Configure retry with backoff
        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(
            new FixedBackOff(1000L, 3)); // Retry 3 times with 1s interval
        factory.setErrorHandler(errorHandler);
        
        return factory;
    }
}
```

**2. Dead Letter Queue Configuration**:
```java
@Configuration
public class KafkaDLQConfig {
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate) {
            
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        
        // Configure DLQ with retry
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (record, exception) -> {
                // Route to a topic based on the original topic
                return new TopicPartition(record.topic() + ".DLT", record.partition());
            });
            
        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(
            recoverer, new FixedBackOff(1000L, 3)); // Retry 3 times before sending to DLQ
            
        factory.setErrorHandler(errorHandler);
        
        return factory;
    }
    
    // Create the DLQ topics
    @Bean
    public NewTopic myTopicDLT() {
        return new NewTopic("my-topic.DLT", 3, (short) 1);
    }
}
```

**3. Advanced Configuration with Exception-Based Routing**:
```java
@Configuration
public class KafkaAdvancedRetryConfig {
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate) {
            
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        
        // Configure different handling based on exception type
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (record, exception) -> {
                if (exception.getCause() instanceof TemporaryException) {
                    // Temporary errors go to retry topic
                    return new TopicPartition(record.topic() + ".retry", record.partition());
                } else {
                    // Permanent errors go to DLQ
                    return new TopicPartition(record.topic() + ".DLT", record.partition());
                }
            });
            
        // Different backoff strategies based on exception
        ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(3);
        expBackOff.setInitialInterval(1000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(10000L);
        
        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(
            recoverer, expBackOff);
            
        // Only retry specific exceptions
        errorHandler.addNotRetryableExceptions(NonRetryableException.class);
        
        factory.setErrorHandler(errorHandler);
        
        return factory;
    }
    
    // Create the retry and DLQ topics
    @Bean
    public NewTopic myTopicRetry() {
        return new NewTopic("my-topic.retry", 3, (short) 1);
    }
    
    @Bean
    public NewTopic myTopicDLT() {
        return new NewTopic("my-topic.DLT", 3, (short) 1);
    }
}
```

**4. Implementing a Retry Topic Consumer**:
```java
@Service
public class RetryTopicConsumer {
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Autowired
    public RetryTopicConsumer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    @KafkaListener(topics = "my-topic.retry", groupId = "retry-group")
    public void consumeRetry(
            @Payload Object message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp) {
            
        long currentTime = System.currentTimeMillis();
        String originalTopic = topic.substring(0, topic.lastIndexOf(".retry"));
        
        // Check if we should retry now or wait
        if (currentTime - timestamp > 60000) { // 1 minute delay
            try {
                // Send back to original topic
                kafkaTemplate.send(originalTopic, message);
                log.info("Retrying message on original topic: {}", originalTopic);
            } catch (Exception e) {
                log.error("Failed to retry message", e);
                // Send to DLQ if retry fails
                kafkaTemplate.send(originalTopic + ".DLT", message);
            }
        } else {
            // Not ready to retry yet, send back to retry topic
            kafkaTemplate.send(topic, message);
        }
    }
}
```

**5. Monitoring and Processing DLQ Messages**:
```java
@Service
public class DLQMonitoringService {
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Autowired
    public DLQMonitoringService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    @KafkaListener(topics = "my-topic.DLT", groupId = "dlq-monitor-group")
    public void monitorDLQ(
            @Payload Object message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage,
            @Header(KafkaHeaders.EXCEPTION_STACKTRACE) String stacktrace) {
            
        // Log DLQ message for monitoring
        log.error("DLQ Message: Topic={}, Partition={}, Timestamp={}, Exception={}", 
                 topic, partition, timestamp, exceptionMessage);
                 
        // Store in database for later analysis or manual reprocessing
        dlqRepository.save(new DLQRecord(topic, partition, timestamp, 
                                        message, exceptionMessage, stacktrace));
                                        
        // Alert if needed
        if (isDLQThresholdExceeded(topic)) {
            alertService.sendAlert("DLQ threshold exceeded for topic: " + topic);
        }
    }
    
    // Manual reprocessing API
    public void reprocessDLQMessage(String dlqRecordId) {
        DLQRecord record = dlqRepository.findById(dlqRecordId)
            .orElseThrow(() -> new NotFoundException("DLQ record not found"));
            
        String originalTopic = record.getTopic().substring(0, record.getTopic().lastIndexOf(".DLT"));
        
        try {
            // Send back to original topic
            kafkaTemplate.send(originalTopic, record.getMessage());
            log.info("Reprocessed DLQ message: {}", dlqRecordId);
            
            // Mark as reprocessed
            record.setReprocessed(true);
            record.setReprocessedTime(LocalDateTime.now());
            dlqRepository.save(record);
        } catch (Exception e) {
            log.error("Failed to reprocess DLQ message", e);
            throw new ReprocessingException("Failed to reprocess: " + e.getMessage());
        }
    }
}
```

**6. Spring Boot 2.6+ Configuration (using RetryTopicConfiguration)**:
```java
@Configuration
public class KafkaRetryTopicConfig {
    
    @Bean
    public RetryTopicConfiguration retryTopicConfiguration(
            KafkaTemplate<String, Object> kafkaTemplate) {
            
        return RetryTopicConfigurationBuilder
            .newInstance()
            .fixedBackOff(1000L) // 1 second backoff
            .maxAttempts(3)
            .includeTopics("my-topic")
            .retryTopicSuffix(".retry")
            .dltSuffix(".DLT")
            .dltHandlerMethod(
                MethodHandles.lookup()
                    .lookupClass()
                    .getDeclaredMethod("handleDltMessage", Object.class, Exception.class))
            .create(kafkaTemplate);
    }
    
    @Bean
    public DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory(
            KafkaTemplate<String, Object> kafkaTemplate) {
        return new DeadLetterPublishingRecovererFactory(kafkaTemplate);
    }
    
    // DLT handler method
    public void handleDltMessage(Object message, Exception exception) {
        log.error("Message sent to DLT: {}, Exception: {}", message, exception.getMessage());
        // Additional handling logic
    }
}
```

This comprehensive approach provides:
- Basic retry with configurable backoff
- Dead letter queues for messages that can't be processed
- Exception-based routing to different topics
- Monitoring and alerting for DLQ messages
- Manual reprocessing capabilities
- Integration with Spring Boot's newer retry topic features

### Q9: How would you test Kafka producers and consumers in Spring Boot?
**Answer:** Testing Kafka producers and consumers in Spring Boot can be done using embedded Kafka for integration tests and mocking for unit tests. Here's a comprehensive approach:

**1. Unit Testing Producers**:
```java
@ExtendWith(MockitoExtension.class)
public class KafkaProducerServiceTest {
    
    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @InjectMocks
    private KafkaProducerService producerService;
    
    @Test
    public void testSendMessage() {
        // Arrange
        String topic = "test-topic";
        TestMessage message = new TestMessage("test-id", "test-content");
        
        when(kafkaTemplate.send(eq(topic), eq(message)))
            .thenReturn(mock(ListenableFuture.class));
        
        // Act
        producerService.sendMessage(topic, message);
        
        // Assert
        verify(kafkaTemplate, times(1)).send(topic, message);
    }
    
    @Test
    public void testSendMessageWithCallback() {
        // Arrange
        String topic = "test-topic";
        TestMessage message = new TestMessage("test-id", "test-content");
        
        ListenableFuture<SendResult<String, Object>> future = mock(ListenableFuture.class);
        when(kafkaTemplate.send(eq(topic), eq(message))).thenReturn(future);
        
        // Act
        producerService.sendMessageWithCallback(topic, message);
        
        // Assert
        verify(kafkaTemplate, times(1)).send(topic, message);
        verify(future, times(1)).addCallback(any(ListenableFutureCallback.class));
    }
}
```

**2. Unit Testing Consumers**:
```java
@ExtendWith(MockitoExtension.class)
public class KafkaConsumerServiceTest {
    
    @Mock
    private UserService userService; // A service that the consumer uses
    
    @InjectMocks
    private KafkaConsumerService consumerService;
    
    @Test
    public void testConsumeUser() {
        // Arrange
        User user = new User("1", "John Doe", "john@example.com");
        
        // Act
        consumerService.consumeUser(user);
        
        // Assert
        verify(userService, times(1)).processUser(user);
    }
    
    @Test
    public void testConsumeWithManualAck() {
        // Arrange
        Object message = new TestMessage("test-id", "test-content");
        Acknowledgment ack = mock(Acknowledgment.class);
        
        // Act
        consumerService.consumeWithManualAck(message, ack);
        
        // Assert
        verify(ack, times(1)).acknowledge();
    }
    
    @Test
    public void testConsumeWithManualAck_WhenException() {
        // Arrange
        Object message = new TestMessage("test-id", "test-content");
        Acknowledgment ack = mock(Acknowledgment.class);
        
        doThrow(new RuntimeException("Processing error"))
            .when(userService).processMessage(any());
        
        // Act & Assert
        assertThrows(RuntimeException.class, () -> {
            consumerService.consumeWithManualAck(message, ack);
        });
        
        verify(ack, never()).acknowledge();
    }
}
```

**3. Integration Testing with Embedded Kafka**:
```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"test-topic"})
public class KafkaIntegrationTest {
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @Autowired
    private KafkaConsumerService consumerService;
    
    @SpyBean
    private UserService userService;
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @BeforeEach
    public void setup() {
        // Configure consumer to use embedded Kafka
        System.setProperty("spring.kafka.bootstrap-servers", bootstrapServers);
    }
    
    @Test
    public void testProducerConsumerIntegration() throws InterruptedException {
        // Arrange
        User user = new User("1", "John Doe", "john@example.com");
        
        // Act
        kafkaTemplate.send("test-topic", user);
        
        // Assert - wait for consumer to process
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(userService, times(1)).processUser(eq(user));
        });
    }
}
```

**4. Testing with TestContainers (for more realistic tests)**:
```java
@SpringBootTest
@Testcontainers
public class KafkaTestContainersTest {
    
    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
    
    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
    }
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @Autowired
    private KafkaConsumerService consumerService;
    
    @SpyBean
    private UserService userService;
    
    @Test
    public void testProducerConsumerWithTestContainers() throws InterruptedException {
        // Arrange
        User user = new User("1", "John Doe", "john@example.com");
        
        // Act
        kafkaTemplate.send("test-topic", user);
        
        // Assert - wait for consumer to process
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(userService, times(1)).processUser(eq(user));
        });
    }
}
```

**5. Testing Error Handling and Retries**:
```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"test-topic", "test-topic.DLT"})
public class KafkaErrorHandlingTest {
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @SpyBean
    private KafkaConsumerService consumerService;
    
    @SpyBean
    private DLQMonitoringService dlqMonitoringService;
    
    @Test
    public void testMessageSentToDLQAfterRetries() throws InterruptedException {
        // Arrange
        TestMessage message = new TestMessage("fail-id", "content-that-causes-failure");
        
        // Configure consumer to fail
        doThrow(new RuntimeException("Processing error"))
            .when(consumerService).consume(eq(message));
        
        // Act
        kafkaTemplate.send("test-topic", message);
        
        // Assert - message should end up in DLQ after retries
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(dlqMonitoringService, times(1)).monitorDLQ(
                eq(message), 
                eq("test-topic.DLT"), 
                anyInt(), 
                anyLong(), 
                contains("Processing error"), 
                any());
        });
    }
}
```

**6. Testing with Custom Serializers/Deserializers**:
```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"test-topic"})
public class KafkaCustomSerializationTest {
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @SpyBean
    private KafkaConsumerService consumerService;
    
    @Test
    public void testCustomSerialization() throws InterruptedException {
        // Arrange
        ComplexObject complexObject = new ComplexObject(/* complex data */);
        
        // Act
        kafkaTemplate.send("test-topic", complexObject);
        
        // Assert - verify object is correctly serialized and deserialized
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<ComplexObject> captor = ArgumentCaptor.forClass(ComplexObject.class);
            verify(consumerService, times(1)).consumeComplex(captor.capture());
            
            ComplexObject received = captor.getValue();
            assertEquals(complexObject.getId(), received.getId());
            assertEquals(complexObject.getName(), received.getName());
            // Assert other properties
        });
    }
}
```

**7. Testing Concurrent Consumers**:
```java
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {"high-volume-topic"})
public class KafkaConcurrentConsumersTest {
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @SpyBean
    private KafkaConsumerService consumerService;
    
    @Test
    public void testConcurrentConsumption() throws InterruptedException {
        // Arrange - send multiple messages
        int messageCount = 10;
        CountDownLatch latch = new CountDownLatch(messageCount);
        
        // Configure consumer to count down latch
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(consumerService).consumeWithConcurrency(any());
        
        // Act - send messages to different partitions
        for (int i = 0; i < messageCount; i++) {
            TestMessage message = new TestMessage("id-" + i, "content-" + i);
            kafkaTemplate.send("high-volume-topic", i % 3, "key-" + i, message);
        }
        
        // Assert - all messages should be consumed concurrently
        assertTrue(latch.await(10, TimeUnit.SECONDS), 
                  "Not all messages were consumed within the timeout");
        verify(consumerService, times(messageCount)).consumeWithConcurrency(any());
    }
}
```

Best practices for Kafka testing:
1. Use unit tests with mocks for simple logic
2. Use embedded Kafka for basic integration tests
3. Use TestContainers for more realistic tests
4. Test error handling and retries
5. Test serialization/deserialization
6. Test concurrent consumption
7. Use appropriate timeouts and waiting mechanisms
8. Clean up resources after tests

## Kafka Architecture and Performance

### Q10: How would you design a Kafka cluster for high throughput and low latency?
**Answer:** Designing a Kafka cluster for high throughput and low latency requires careful consideration of hardware, configuration, and architecture. Here's a comprehensive approach:

**1. Hardware Considerations**:
- **CPU**: Multi-core processors (16+ cores) for parallel processing
- **Memory**: 
  - 32-64GB RAM per broker
  - Allocate 5GB heap to Kafka (more doesn't help due to GC)
  - Rest for page cache (critical for performance)
- **Disk**:
  - SSDs for highest performance (NVMe if possible)
  - RAID 10 for HDDs if SSDs not feasible
  - Separate disks for OS and Kafka data
  - Multiple mount points for log.dirs to distribute I/O
- **Network**:
  - 10Gbps+ network interfaces
  - Redundant network paths
  - Low-latency network equipment

**2. Broker Configuration**:
- **Partitions**:
  - Formula: Total partitions = (Throughput in MB/s * Replication factor) / (10 MB/s)
  - Typically 2000-4000 partitions per broker maximum
  - Balance between parallelism and overhead
- **Replication**:
  - Use replication factor 3 for critical data
  - min.insync.replicas=2 for durability
- **Batch Settings**:
  - Increase batch.size (128KB-1MB)
  - Optimize linger.ms (5-100ms) based on latency requirements
- **Compression**:
  - Enable compression (typically lz4 or zstd)
  - compression.type=producer to offload compression to clients
- **Flush Settings**:
  - log.flush.interval.messages and log.flush.interval.ms set high
  - Rely on OS page cache and background flushes
- **Retention**:
  - Set appropriate log.retention.hours/bytes
  - Use log.cleanup.policy=compact for key-based datasets
- **Thread Pools**:
  - num.network.threads=cpu_cores
  - num.io.threads=cpu_cores*2
- **Socket Settings**:
  - socket.send.buffer.bytes and socket.receive.buffer.bytes (1MB+)
  - socket.request.max.bytes (100MB+)

**3. Producer Configuration**:
- **Batching**:
  - batch.size=16KB-128KB
  - linger.ms=5-100ms (trade-off between latency and throughput)
- **Compression**:
  - compression.type=lz4 or zstd
- **Buffering**:
  - buffer.memory=32MB-1GB
- **Acknowledgments**:
  - acks=1 for performance with some durability
  - acks=all for critical data
- **Retries**:
  - retries=MAX_INT
  - retry.backoff.ms=100-500
- **Idempotence**:
  - enable.idempotence=true for exactly-once semantics
- **Parallelism**:
  - max.in.flight.requests.per.connection=5 (with idempotence)

**4. Consumer Configuration**:
- **Fetch Size**:
  - fetch.min.bytes=1KB-1MB
  - fetch.max.bytes=50MB+
  - max.partition.fetch.bytes=1MB-10MB
- **Polling**:
  - max.poll.records=500-1000
  - max.poll.interval.ms=300000 (5 minutes)
- **Commit Strategy**:
  - enable.auto.commit=false for more control
  - Manual commits after processing
- **Prefetching**:
  - fetch.max.wait.ms=500

**5. Cluster Architecture**:
- **Broker Count**:
  - Start with 3-6 brokers
  - Scale horizontally as needed
- **Partition Distribution**:
  - Distribute partitions evenly across brokers
  - Use custom partition assignment strategies if needed
- **Topic Design**:
  - Create topics with appropriate partition counts
  - Consider message key distribution for even load
- **Rack Awareness**:
  - Distribute brokers across racks/AZs
  - Enable rack.aware.replica.selector=true
- **ZooKeeper**:
  - Separate ZooKeeper ensemble (3-5 nodes)
  - Or use KRaft mode (Kafka 3.0+) to eliminate ZooKeeper

**6. Monitoring and Tuning**:
- **Metrics to Watch**:
  - Under-replicated partitions
  - Request latency
  - Broker CPU, disk, network utilization
  - Producer/consumer lag
  - Batch sizes and compression ratios
- **Tools**:
  - Prometheus + Grafana
  - Confluent Control Center
  - LinkedIn's Cruise Control for automated balancing

**7. Advanced Techniques**:
- **Tiered Storage**:
  - Use Kafka's tiered storage (Kafka 2.8+) for historical data
- **Quotas**:
  - Set client quotas to prevent resource hogging
- **Geo-Replication**:
  - MirrorMaker 2 for multi-datacenter replication
- **Partitioning Strategy**:
  - Custom partitioners for even key distribution
- **Consumer Group Design**:
  - Balance consumer instances and partitions
  - Consider sticky partition assignment

**8. Real-World Optimizations**:
- **Message Size**:
  - Keep messages small (ideally <1MB)
  - Use references to external storage for large objects
- **Topic Segregation**:
  - Separate high-throughput and low-latency topics
  - Consider dedicated brokers for critical topics
- **Client Distribution**:
  - Distribute client load across brokers
  - Use multiple producer instances
- **Batch Processing**:
  - Implement application-level batching
  - Use async producers where possible

**9. Latency-Specific Optimizations**:
- Reduce linger.ms to minimum
- Use acks=1 instead of acks=all
- Place brokers in same rack as producers for critical paths
- Optimize JVM GC settings (G1GC with tuned parameters)
- Consider dedicated NICs for Kafka traffic
- Use partitions with keys that distribute evenly

**10. Throughput-Specific Optimizations**:
- Increase batch sizes
- Maximize compression
- Use higher linger.ms values
- Increase fetch sizes for consumers
- Optimize consumer processing with parallelism
- Consider increasing num.replica.fetchers

By carefully implementing these design considerations, you can build a Kafka cluster capable of handling millions of messages per second with sub-10ms latency.

### Q11: Explain Kafka's storage architecture and how it achieves high performance.
**Answer:** Kafka's storage architecture is fundamental to its high performance. It uses a distributed commit log as its core abstraction and employs several optimizations to achieve exceptional throughput and low latency.

**Core Storage Architecture**:

1. **Log-Based Storage**:
   - Each topic partition is a logical log
   - Messages are appended sequentially to the end of the log
   - Immutable once written
   - Identified by sequential offset numbers

2. **Segment Files**:
   - Each partition log is divided into segments
   - Default segment size is 1GB or 1 week of data
   - Active segment is the one being written to
   - Older segments are read-only
   - Segments are named by the offset of the first message they contain

3. **File Format**:
   - Simple format with message batches
   - Each message batch contains:
     - Timestamp
     - Compression codec
     - CRC32 checksum
     - Key/value size
     - Key/value payload
   - Index files for fast offset lookup

**Performance Optimizations**:

1. **Sequential I/O**:
   - Append-only log structure enables sequential disk writes
   - Sequential I/O is much faster than random access (even on SSDs)
   - Allows Kafka to approach raw disk throughput

2. **Zero-Copy Data Transfer**:
   - Uses sendfile() system call to transfer data directly from disk to network
   - Avoids copying data between user space and kernel space
   - Reduces CPU usage and increases throughput

3. **Page Cache Utilization**:
   - Relies heavily on the OS page cache
   - Avoids maintaining a separate cache in the JVM heap
   - Prevents double-buffering
   - Allows Kafka to serve data from memory when available

4. **Batching**:
   - Groups messages into batches for producers and consumers
   - Amortizes the overhead of network and disk operations
   - Increases throughput at the cost of slight latency

5. **Compression**:
   - Compresses message batches (not individual messages)
   - Supports multiple codecs (gzip, snappy, lz4, zstd)
   - Reduces storage requirements and network bandwidth
   - Improves I/O throughput

6. **Partitioning**:
   - Distributes topic data across multiple brokers
   - Enables parallel processing
   - Allows horizontal scaling

7. **Efficient Message Format**:
   - Compact binary format
   - Minimal overhead per message
   - Optimized for both small and large messages

**Read Efficiency**:

1. **Offset-Based Retrieval**:
   - Consumers maintain their position (offset) in the log
   - Allows efficient resumption after failures

2. **Index Files**:
   - Each segment has an associated index file
   - Maps offsets to physical file positions
   - Enables O(log n) lookup time for random access

3. **Time-Based Index**:
   - Additional index mapping timestamps to offsets
   - Enables efficient time-based queries

4. **Predictable Read Patterns**:
   - Consumers typically read sequentially
   - Allows for effective read-ahead by the OS

**Write Efficiency**:

1. **Append-Only Log**:
   - No random writes or updates
   - No read-modify-write cycles
   - Maximizes disk throughput

2. **Group Commit**:
   - Batches multiple writes into single I/O operations
   - Reduces disk operations

3. **Asynchronous Writes**:
   - Producers can send messages asynchronously
   - Allows pipelining of requests

4. **Delayed Flush**:
   - Relies on OS page cache for immediate persistence
   - Configurable flush intervals
   - Trades durability for performance

**Replication Efficiency**:

1. **Leader-Based Replication**:
   - Only the leader handles client reads/writes
   - Followers pull data from the leader
   - Reduces complexity and improves performance

2. **Pull-Based Replication**:
   - Followers pull data at their own pace
   - Prevents slow followers from affecting the leader
   - Similar to consumer protocol for simplicity

3. **Batch Replication**:
   - Followers fetch multiple messages in a single request
   - Reduces network overhead

**Deletion and Retention**:

1. **Segment-Based Deletion**:
   - Entire segments are deleted when expired
   - Avoids fragmentation
   - Efficient for time-based retention

2. **Log Compaction**:
   - For key-based retention
   - Retains at least the last state for each key
   - Efficient background process

**Storage Hierarchy**:

1. **Broker**:
   - Contains multiple topics
   - Manages disk allocation

2. **Topic**:
   - Logical grouping of related data
   - Contains multiple partitions

3. **Partition**:
   - Unit of parallelism
   - Contains multiple segments

4. **Segment**:
   - Physical file on disk
   - Contains message batches

5. **Batch**:
   - Group of messages
   - Unit of compression

6. **Message**:
   - Individual record
   - Contains key, value, timestamp, headers

This architecture allows Kafka to:
- Handle millions of messages per second on modest hardware
- Provide consistent performance regardless of data size
- Maintain low latency for both producers and consumers
- Scale horizontally by adding more brokers
- Efficiently utilize modern hardware capabilities

### Q12: What are the key metrics you would monitor in a Kafka cluster and how would you address performance issues?
**Answer:** Monitoring a Kafka cluster effectively requires tracking a comprehensive set of metrics across brokers, producers, consumers, and the overall system. Here's a detailed approach to monitoring Kafka and addressing performance issues:

**Key Metrics to Monitor**:

1. **Broker Metrics**:
   - **Under-replicated Partitions**: Number of partitions with replicas that are not in-sync
   - **Offline Partitions**: Partitions without an active leader
   - **Active Controller Count**: Should be exactly 1 across the cluster
   - **Request Rate**: Requests per second by type (produce, fetch, metadata)
   - **Request Latency**: Time to process requests (p99, p95, average)
   - **Request Queue Time**: Time requests spend in queue before processing
   - **Network Throughput**: Bytes in/out per second
   - **Disk Utilization**: Bytes written/read per second
   - **Log Flush Rate and Latency**: How often and how long it takes to flush to disk
   - **Replica Fetch Rate**: Rate at which followers fetch from leaders
   - **ISR Shrinks/Expands**: Frequency of in-sync replica set changes

2. **JVM Metrics**:
   - **Garbage Collection**: GC pause times and frequency
   - **Heap Usage**: Current and max heap utilization
   - **Non-heap Memory**: Off-heap memory usage
   - **Thread Count**: Number of active threads

3. **OS Metrics**:
   - **CPU Usage**: Overall and per-core utilization
   - **Disk I/O**: IOPS, throughput, and utilization
   - **Disk Space**: Free space on Kafka data directories
   - **Network**: Throughput, packet loss, retransmits
   - **Page Cache**: Hit ratio and utilization
   - **Open File Handles**: Number of open file descriptors

4. **Topic and Partition Metrics**:
   - **Message Rate**: Messages per second per topic/partition
   - **Bytes Rate**: Throughput per topic/partition
   - **Partition Size**: Disk usage per partition
   - **Partition Count**: Number of partitions per broker
   - **Leader Count**: Number of partitions for which a broker is the leader

5. **Producer Metrics**:
   - **Produce Rate**: Records sent per second
   - **Produce Latency**: Time to acknowledge messages
   - **Batch Size**: Average and max batch size
   - **Record Queue Time**: Time records spend in producer queue
   - **Record Error Rate**: Failed record sends
   - **Compression Rate**: Ratio of compressed to uncompressed data

6. **Consumer Metrics**:
   - **Consumer Lag**: Difference between latest offset and consumer position
   - **Consumption Rate**: Records consumed per second
   - **Fetch Rate**: Number of fetch requests per second
   - **Fetch Size**: Average and max fetch size
   - **Commit Rate**: Frequency of offset commits
   - **Rebalance Rate**: Frequency of consumer group rebalances
   - **Join Time**: Time to join consumer group

7. **ZooKeeper Metrics** (if not using KRaft):
   - **Latency**: Request latency
   - **Packet Count**: Number of packets sent/received
   - **Connection Count**: Number of client connections
   - **Watch Count**: Number of watches
   - **Ensemble State**: Leader/follower status

**Monitoring Tools**:

1. **JMX Metrics Collection**:
   - Prometheus with JMX Exporter
   - Datadog
   - New Relic
   - Dynatrace

2. **Visualization**:
   - Grafana dashboards
   - Confluent Control Center
   - Kafka Manager/CMAK
   - Kafka Tool

3. **Alerting**:
   - AlertManager
   - PagerDuty
   - OpsGenie
   - Custom alerting rules

**Addressing Common Performance Issues**:

1. **High Producer Latency**:
   - **Symptoms**: Increased produce request latency, producer throttling
   - **Potential Causes**:
     - Insufficient broker resources
     - Network congestion
     - Slow disk I/O
     - Large batches with acks=all
   - **Solutions**:
     - Increase broker resources (CPU, memory, network)
     - Optimize producer batch settings
     - Consider reducing replication factor
     - Adjust acks setting for less critical data
     - Enable compression
     - Check for network bottlenecks

2. **High Consumer Lag**:
   - **Symptoms**: Growing consumer lag, slow consumption rate
   - **Potential Causes**:
     - Slow consumer processing
     - Insufficient consumer parallelism
     - Network bottlenecks
     - Broker overload
   - **Solutions**:
     - Add more consumer instances
     - Optimize consumer processing logic
     - Increase fetch size
     - Implement consumer-side batching
     - Rebalance partitions
     - Scale consumer group

3. **Under-replicated Partitions**:
   - **Symptoms**: Non-zero under-replicated partitions metric
   - **Potential Causes**:
     - Network issues between brokers
     - Broker overload
     - Disk failures
     - Insufficient replication bandwidth
   - **Solutions**:
     - Check network connectivity
     - Increase num.replica.fetchers
     - Check disk health
     - Reduce load on affected brokers
     - Adjust replica.lag.time.max.ms

4. **Broker CPU Saturation**:
   - **Symptoms**: High CPU usage, increased request latency
   - **Potential Causes**:
     - Too many partitions
     - High request rate
     - Inefficient message format
     - GC pressure
   - **Solutions**:
     - Add more brokers
     - Optimize JVM settings
     - Reduce partition count per broker
     - Enable compression
     - Check for inefficient client usage patterns

5. **Disk I/O Bottlenecks**:
   - **Symptoms**: High disk utilization, increased flush latency
   - **Potential Causes**:
     - Slow disks
     - Too many partitions on same disk
     - Competing I/O workloads
     - Small I/O operations
   - **Solutions**:
     - Use faster storage (SSDs)
     - Distribute partitions across multiple disks
     - Isolate Kafka disks from other workloads
     - Adjust log flush settings
     - Optimize retention policies

6. **Network Saturation**:
   - **Symptoms**: High network utilization, increased latency
   - **Potential Causes**:
     - Insufficient network bandwidth
     - Large message sizes
     - Uneven partition distribution
     - Cross-datacenter traffic
   - **Solutions**:
     - Upgrade network infrastructure
     - Enable compression
     - Balance partitions across brokers
     - Implement edge aggregation
     - Consider rack awareness

7. **Memory Pressure**:
   - **Symptoms**: High heap usage, frequent GC pauses
   - **Potential Causes**:
     - Inefficient memory settings
     - Too many connections
     - Large message batches
     - Memory leaks
   - **Solutions**:
     - Optimize JVM settings
     - Limit client connections
     - Adjust fetch and produce sizes
     - Upgrade to newer Kafka versions
     - Monitor and tune GC

8. **Unbalanced Cluster**:
   - **Symptoms**: Uneven load across brokers
   - **Potential Causes**:
     - Poor partition assignment
     - Skewed key distribution
     - Broker additions/removals
   - **Solutions**:
     - Use Kafka's partition reassignment tools
     - Implement Cruise Control
     - Design better partitioning keys
     - Regularly rebalance the cluster

9. **ZooKeeper Issues** (if not using KRaft):
   - **Symptoms**: Slow metadata operations, controller failovers
   - **Potential Causes**:
     - ZooKeeper ensemble overload
     - Network issues
     - Insufficient ZooKeeper resources
   - **Solutions**:
     - Scale ZooKeeper ensemble
     - Optimize ZooKeeper settings
     - Isolate ZooKeeper traffic
     - Consider migrating to KRaft mode

10. **Consumer Group Rebalancing**:
    - **Symptoms**: Frequent rebalances, processing pauses
    - **Potential Causes**:
      - Consumer timeouts
      - Network issues
      - Slow processing
    - **Solutions**:
      - Increase session.timeout.ms
      - Adjust max.poll.interval.ms
      - Optimize consumer processing
      - Implement static group membership

**Proactive Performance Tuning**:

1. **Regular Capacity Planning**:
   - Monitor growth trends
   - Plan for scaling before hitting limits
   - Test with projected future loads

2. **Performance Testing**:
   - Benchmark with production-like workloads
   - Test failure scenarios
   - Validate configuration changes

3. **Configuration Reviews**:
   - Regularly review and update configurations
   - Apply best practices
   - Stay updated with new Kafka versions

4. **Automated Monitoring and Remediation**:
   - Implement predictive alerts
   - Automate common remediation tasks
   - Use tools like Cruise Control for automated balancing

By systematically monitoring these metrics and applying the appropriate remediation strategies, you can maintain a high-performance Kafka cluster even under challenging conditions.

## Kafka Resilience Patterns

### Q13: Explain different strategies for handling failures in Kafka-based microservices.
**Answer:** Handling failures in Kafka-based microservices requires implementing various resilience patterns. Here's a comprehensive overview of strategies for different failure scenarios:

**1. Producer-Side Resilience**:

- **Retry Mechanism**:
  - Configure `retries` parameter (typically set high or to MAX_INT)
  - Set appropriate `retry.backoff.ms` (e.g., 100-500ms with exponential backoff)
  - Use `delivery.timeout.ms` to cap total retry time

- **Idempotent Producers**:
  - Enable with `enable.idempotence=true`
  - Prevents duplicate messages due to retries
  - Assigns sequence numbers to detect duplicates

- **Acknowledgment Levels**:
  - `acks=all`: Wait for all in-sync replicas (most durable)
  - `acks=1`: Wait only for leader (balance of performance and durability)
  - `acks=0`: Fire and forget (highest throughput, no durability guarantees)

- **Local Buffering**:
  - Buffer messages locally before sending
  - Implement disk-based queuing for critical data
  - Resume sending after recovery

- **Circuit Breaker Pattern**:
  - Track failure rates
  - Stop sending after threshold is reached
  - Periodically test if system has recovered
  - Gradually restore normal operation

- **Timeout Management**:
  - Set appropriate `request.timeout.ms`
  - Implement application-level timeouts
  - Handle timeout exceptions gracefully

**2. Consumer-Side Resilience**:

- **Offset Management**:
  - Commit offsets only after successful processing
  - Use manual commit mode (`enable.auto.commit=false`)
  - Implement exactly-once semantics with transactional processing

- **Error Handling**:
  - Implement try-catch blocks around message processing
  - Categorize errors as retriable vs. non-retriable
  - Log detailed error information

- **Dead Letter Queue (DLQ)**:
  - Send failed messages to a dedicated error topic
  - Include original topic, partition, offset, and error details
  - Implement monitoring and alerting for DLQ
  - Create tools for reprocessing DLQ messages

- **Poison Pill Detection**:
  - Identify messages that consistently cause failures
  - Move to DLQ after N retries
  - Alert operations team

- **Backpressure Handling**:
  - Adjust `max.poll.records` based on processing capacity
  - Implement flow control mechanisms
  - Pause/resume consumption based on downstream system health

- **Consumer Rebalance Handling**:
  - Implement `ConsumerRebalanceListener`
  - Save processing state before rebalance
  - Resume processing after rebalance
  - Consider static group membership for faster rebalances

**3. System-Level Resilience**:

- **Kafka Cluster Redundancy**:
  - Deploy across multiple availability zones/racks
  - Use appropriate replication factor (typically 3)
  - Set `min.insync.replicas=2` for critical topics

- **Multi-Region Deployment**:
  - Implement MirrorMaker 2 for cross-region replication
  - Active-passive or active-active configurations
  - Consider Confluent Replicator for managed environments

- **Topic Design for Resilience**:
  - Use appropriate partition counts for parallelism
  - Consider compacted topics for state recovery
  - Implement retention policies based on recovery needs

- **Monitoring and Alerting**:
  - Monitor consumer lag
  - Alert on under-replicated partitions
  - Track error rates and DLQ growth
  - Implement predictive monitoring

**4. Application-Level Resilience Patterns**:

- **Saga Pattern**:
  - Break long-running transactions into smaller steps
  - Implement compensating transactions for rollback
  - Use dedicated topics for saga orchestration

- **Event Sourcing**:
  - Store all state changes as events
  - Rebuild state from event log
  - Use compacted topics for current state

- **CQRS (Command Query Responsibility Segregation)**:
  - Separate write and read models
  - Use Kafka as the event backbone
  - Rebuild read models from event streams

- **Outbox Pattern**:
  - Store outgoing messages in a database "outbox" table
  - Use a separate process to publish from outbox to Kafka
  - Ensures consistency between database and Kafka operations

- **Materialized View Pattern**:
  - Maintain read-optimized views of data
  - Rebuild views from Kafka topics if corrupted
  - Use Kafka Streams or ksqlDB for view maintenance

**5. Specific Failure Scenarios and Mitigations**:

- **Network Partitions**:
  - Configure appropriate timeouts
  - Implement client-side retries
  - Use rack-aware replica placement

- **Broker Failures**:
  - Rely on Kafka's leader election
  - Configure `unclean.leader.election.enable=false` for data consistency
  - Monitor and replace failed brokers

- **Consumer Process Crashes**:
  - Implement graceful shutdown hooks
  - Commit offsets before shutdown
  - Design for quick restart and recovery

- **Database Failures in Consuming Applications**:
  - Implement the Outbox pattern
  - Use local transaction logs
  - Pause consumption until database recovers

- **Slow Consumers**:
  - Monitor consumer lag
  - Scale out consumer groups
  - Implement backpressure mechanisms
  - Consider separate topics for different SLA requirements

**6. Implementation Examples**:

- **Spring Boot Retry Configuration**:
```java
@Configuration
public class KafkaRetryConfig {
    @Bean
    public RetryTopicConfiguration retryTopicConfiguration(
            KafkaTemplate<String, Object> kafkaTemplate) {
        return RetryTopicConfigurationBuilder
            .newInstance()
            .exponentialBackoff(1000, 2, 30000) // Initial delay, multiplier, max delay
            .maxAttempts(5)
            .includeTopics("orders-topic")
            .create(kafkaTemplate);
    }
}
```

- **Dead Letter Queue with Error Handling**:
```java
@KafkaListener(topics = "orders-topic", groupId = "order-processing-group")
public void processOrder(ConsumerRecord<String, Order> record, 
                         Acknowledgment ack) {
    try {
        orderService.processOrder(record.value());
        ack.acknowledge();
    } catch (RetryableException e) {
        // Retry by not acknowledging
        log.warn("Retryable error processing order", e);
        throw e; // Will be retried by Kafka
    } catch (Exception e) {
        log.error("Non-retryable error processing order", e);
        // Send to DLQ
        kafkaTemplate.send("orders-topic-dlt", 
                          record.key(), 
                          record.value());
        // Record the error details
        errorRepository.save(new ProcessingError(
            record.topic(), 
            record.partition(), 
            record.offset(),
            e.getMessage(),
            record.value()
        ));
        ack.acknowledge(); // Acknowledge to move past this message
    }
}
```

- **Circuit Breaker Implementation**:
```java
@Service
public class ResilientKafkaProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final CircuitBreaker circuitBreaker;
    
    public ResilientKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        
        // Configure circuit breaker
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
            .failureRateThreshold(50)
            .waitDurationInOpenState(Duration.ofSeconds(60))
            .permittedNumberOfCallsInHalfOpenState(10)
            .slidingWindowSize(100)
            .build();
            
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
        this.circuitBreaker = registry.circuitBreaker("kafka-producer");
    }
    
    public CompletableFuture<SendResult<String, Object>> send(String topic, 
                                                             String key, 
                                                             Object value) {
        return Try.ofSupplier(() -> 
            circuitBreaker.executeSupplier(() -> 
                kafkaTemplate.send(topic, key, value).completable()
            )
        ).recover(throwable -> {
            // Handle circuit breaker open or other failures
            log.error("Failed to send message to Kafka", throwable);
            // Store in local buffer for later retry
            bufferService.buffer(topic, key, value);
            return CompletableFuture.failedFuture(throwable);
        }).get();
    }
}
```

- **Outbox Pattern Implementation**:
```java
@Transactional
public void createOrder(Order order) {
    // Save the order to the database
    Order savedOrder = orderRepository.save(order);
    
    // Create an outbox event
    OutboxEvent event = OutboxEvent.builder()
        .aggregateType("Order")
        .aggregateId(savedOrder.getId().toString())
        .eventType("OrderCreated")
        .payload(objectMapper.writeValueAsString(savedOrder))
        .build();
        
    // Save the event to the outbox table in the same transaction
    outboxRepository.save(event);
}

// Separate process/thread to publish from outbox to Kafka
@Scheduled(fixedRate = 1000)
public void publishOutboxEvents() {
    List<OutboxEvent> unpublishedEvents = outboxRepository.findUnpublishedEvents();
    
    for (OutboxEvent event : unpublishedEvents) {
        try {
            kafkaTemplate.send(event.getAggregateType().toLowerCase() + "-events", 
                              event.getAggregateId(), 
                              event.getPayload())
                .get(); // Wait for send to complete
                
            event.setPublished(true);
            outboxRepository.save(event);
        } catch (Exception e) {
            log.error("Failed to publish outbox event", e);
            // Will retry on next schedule
        }
    }
}
```

These resilience strategies, when properly implemented and combined, create a robust Kafka-based microservices architecture that can withstand various failure scenarios while maintaining data integrity and system availability.

### Q14: Describe the concept of "exactly-once" processing in Kafka and how to implement it.
**Answer:** "Exactly-once" processing is a critical concept in distributed systems that ensures each message is processed exactly once, even in the face of failures. In Kafka, achieving true exactly-once semantics involves multiple components working together.

**Understanding Message Delivery Semantics**:

1. **At-Most-Once**: Messages may be lost but never reprocessed
   - Achieved with `enable.auto.commit=true` and no retries
   - Lowest overhead but no guarantees against data loss

2. **At-Least-Once**: Messages are never lost but may be reprocessed
   - Achieved with manual commits after processing
   - Guarantees against data loss but may cause duplicates

3. **Exactly-Once**: Messages are neither lost nor reprocessed
   - The most challenging to implement
   - Requires coordination between producers, brokers, and consumers

**Kafka's Exactly-Once Semantics (EOS)**:

Kafka introduced true exactly-once semantics in version 0.11.0 through several key features:

1. **Idempotent Producers**:
   - Prevents duplicate messages due to producer retries
   - Each producer gets a unique ID (PID)
   - Each message gets a sequence number
   - Brokers use PID and sequence numbers to detect and reject duplicates

2. **Transactions API**:
   - Allows atomic writes across multiple partitions and topics
   - Enables consuming, processing, and producing in a single transaction
   - Introduces transaction coordinator and transaction log

3. **Consumer Transaction Isolation**:
   - Consumers can be configured to read only committed messages
   - Prevents reading uncommitted or aborted messages

**Implementing Exactly-Once Processing**:

1. **Producer Configuration**:
```java
Properties producerProps = new Properties();
producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

// Enable idempotence
producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

// Set acks to all (required for idempotence)
producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

// Configure transactions
producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");

// Create producer
KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

// Initialize transactions
producer.initTransactions();
```

2. **Consumer Configuration**:
```java
Properties consumerProps = new Properties();
consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");

// Read only committed messages
consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

// Disable auto-commit
consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

// Create consumer
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
```

3. **Transactional Consume-Transform-Produce**:
```java
// Subscribe to input topic
consumer.subscribe(Collections.singletonList("input-topic"));

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    
    if (!records.isEmpty()) {
        // Begin transaction
        producer.beginTransaction();
        
        try {
            // Process records and produce results
            for (ConsumerRecord<String, String> record : records) {
                // Process the record
                String transformedValue = processRecord(record.value());
                
                // Produce to output topic
                producer.send(new ProducerRecord<>("output-topic", record.key(), transformedValue));
            }
            
            // Commit consumer offsets within transaction
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
            }
            
            // Send offsets to transaction
            producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
            
            // Commit transaction
            producer.commitTransaction();
        } catch (Exception e) {
            // Abort transaction on error
            producer.abortTransaction();
            // Handle the exception
            handleException(e);
        }
    }
}
```

4. **Spring Kafka Implementation**:
```java
@Configuration
public class KafkaConfig {
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-");
        
        return new DefaultKafkaProducerFactory<>(props);
    }
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }
    
    @Bean
    public KafkaTransactionManager<String, String> kafkaTransactionManager(
            ProducerFactory<String, String> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }
    
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(
            ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            KafkaTransactionManager<String, String> kafkaTransactionManager) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setTransactionManager(kafkaTransactionManager);
        return factory;
    }
}

@Service
public class ExactlyOnceService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    
    public ExactlyOnceService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    @Transactional
    @KafkaListener(topics = "input-topic", groupId = "exactly-once-group")
    public void processMessage(ConsumerRecord<String, String> record, 
                              Acknowledgment ack) {
        // Process the record
        String transformedValue = processRecord(record.value());
        
        // Produce to output topic - this will be part of the same transaction
        kafkaTemplate.send("output-topic", record.key(), transformedValue);
        
        // No need to manually commit offsets - handled by transaction manager
    }
}
```

**Limitations and Considerations**:

1. **Performance Impact**:
   - Transactions add overhead (10-20% throughput reduction)
   - Additional network calls to transaction coordinator
   - Consider if your use case truly requires exactly-once

2. **Transactional ID Management**:
   - Each producer instance needs a unique transactional ID
   - IDs should be consistent across restarts for the same logical producer
   - Consider using application instance ID + topic + partition

3. **Transaction Timeout**:
   - Default transaction timeout is 15 minutes
   - Configure `transaction.timeout.ms` based on processing needs
   - Long-running transactions increase the risk of timeouts

4. **Fencing Mechanism**:
   - Kafka uses an epoch number to prevent "zombie" producers
   - Old producer instances are fenced off when a new instance with the same transactional ID starts

5. **Consumer Group Considerations**:
   - All consumers in a group should use the same isolation level
   - Group rebalances can complicate exactly-once processing

6. **External Systems Integration**:
   - True end-to-end exactly-once requires coordination with external systems
   - Consider using the Outbox Pattern for database integration

**Advanced Patterns for Exactly-Once**:

1. **Outbox Pattern with Transactions**:
```java
@Transactional
public void processOrderWithExactlyOnce(Order order) {
    // Update database
    orderRepository.save(order);
    
    // Create outbox event in the same transaction
    OutboxEvent event = new OutboxEvent(
        UUID.randomUUID(),
        "Order",
        order.getId(),
        "OrderProcessed",
        objectMapper.writeValueAsString(order)
    );
    outboxRepository.save(event);
}

@Transactional
@Scheduled(fixedRate = 1000)
public void publishOutboxEvents() {
    List<OutboxEvent> events = outboxRepository.findUnpublishedEvents(100);
    
    if (!events.isEmpty()) {
        kafkaTemplate.executeInTransaction(template -> {
            for (OutboxEvent event : events) {
                template.send("order-events", event.getAggregateId(), event.getPayload());
                event.setPublished(true);
                outboxRepository.save(event);
            }
            return null;
        });
    }
}
```

2. **Idempotent Consumers**:
```java
@Service
public class IdempotentConsumer {
    private final ProcessedEventRepository repository;
    
    @Transactional
    @KafkaListener(topics = "order-events", groupId = "idempotent-group")
    public void processEvent(ConsumerRecord<String, String> record) {
        String eventId = extractEventId(record.value());
        
        // Check if already processed
        if (repository.existsByEventId(eventId)) {
            log.info("Event already processed: {}", eventId);
            return;
        }
        
        // Process the event
        processEvent(record.value());
        
        // Mark as processed
        repository.save(new ProcessedEvent(eventId));
    }
}
```

3. **Exactly-Once with Kafka Streams**:
```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exactly-once-app");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

StreamsBuilder builder = new StreamsBuilder();
builder.stream("input-topic")
       .mapValues(value -> processRecord(value))
       .to("output-topic");

KafkaStreams streams = new KafkaStreams(builder.build(), props);
streams.start();
```

By combining these techniques and understanding the underlying mechanisms, you can implement robust exactly-once processing in Kafka-based applications, ensuring data integrity even in distributed environments with failures.

### Q15: How would you implement a distributed transaction across multiple services using Kafka?
**Answer:** Implementing distributed transactions across multiple microservices using Kafka requires careful design to ensure consistency without sacrificing the benefits of a distributed architecture. There are several patterns to achieve this, each with different trade-offs.

**1. Saga Pattern**:

The Saga pattern is one of the most common approaches for distributed transactions in microservices. It breaks down a long-running transaction into a sequence of local transactions, each with compensating actions for rollback.

**Implementation Approach**:

- **Choreography-based Saga**:
  ```java
  // Order Service
  @Service
  public class OrderService {
      private final KafkaTemplate<String, Object> kafkaTemplate;
      
      @Transactional
      public void createOrder(Order order) {
          // Save order with PENDING status
          orderRepository.save(order);
          
          // Publish order created event
          OrderCreatedEvent event = new OrderCreatedEvent(order.getId(), order.getCustomerId(), 
                                                         order.getAmount(), order.getItems());
          kafkaTemplate.send("order-events", order.getId(), event);
      }
      
      @KafkaListener(topics = "payment-events")
      public void handlePaymentEvent(PaymentEvent event) {
          Order order = orderRepository.findById(event.getOrderId()).orElseThrow();
          
          if (event.getStatus() == PaymentStatus.COMPLETED) {
              order.setStatus(OrderStatus.PAYMENT_COMPLETED);
              orderRepository.save(order);
              
              // Trigger inventory reservation
              OrderPaymentCompletedEvent nextEvent = new OrderPaymentCompletedEvent(order.getId(), order.getItems());
              kafkaTemplate.send("order-payment-events", order.getId(), nextEvent);
          } else if (event.getStatus() == PaymentStatus.FAILED) {
              order.setStatus(OrderStatus.PAYMENT_FAILED);
              orderRepository.save(order);
              
              // Publish order cancelled event
              OrderCancelledEvent cancelEvent = new OrderCancelledEvent(order.getId(), "Payment failed");
              kafkaTemplate.send("order-cancelled-events", order.getId(), cancelEvent);
          }
      }
      
      @KafkaListener(topics = "inventory-events")
      public void handleInventoryEvent(InventoryEvent event) {
          Order order = orderRepository.findById(event.getOrderId()).orElseThrow();
          
          if (event.getStatus() == InventoryStatus.RESERVED) {
              order.setStatus(OrderStatus.INVENTORY_RESERVED);
              orderRepository.save(order);
              
              // Publish order completed event
              OrderCompletedEvent completedEvent = new OrderCompletedEvent(order.getId());
              kafkaTemplate.send("order-completed-events", order.getId(), completedEvent);
          } else if (event.getStatus() == InventoryStatus.FAILED) {
              order.setStatus(OrderStatus.INVENTORY_FAILED);
              orderRepository.save(order);
              
              // Trigger payment refund
              RefundPaymentEvent refundEvent = new RefundPaymentEvent(order.getId(), order.getAmount());
              kafkaTemplate.send("payment-refund-events", order.getId(), refundEvent);
              
              // Publish order cancelled event
              OrderCancelledEvent cancelEvent = new OrderCancelledEvent(order.getId(), "Inventory reservation failed");
              kafkaTemplate.send("order-cancelled-events", order.getId(), cancelEvent);
          }
      }
  }
  
  // Payment Service
  @Service
  public class PaymentService {
      private final KafkaTemplate<String, Object> kafkaTemplate;
      
      @KafkaListener(topics = "order-events")
      @Transactional
      public void processPayment(OrderCreatedEvent event) {
          try {
              // Process payment
              Payment payment = new Payment(event.getOrderId(), event.getCustomerId(), event.getAmount());
              paymentRepository.save(payment);
              
              // Call payment gateway
              PaymentResult result = paymentGateway.processPayment(payment);
              
              if (result.isSuccessful()) {
                  payment.setStatus(PaymentStatus.COMPLETED);
                  paymentRepository.save(payment);
                  
                  // Publish payment completed event
                  PaymentCompletedEvent completedEvent = new PaymentCompletedEvent(
                      event.getOrderId(), PaymentStatus.COMPLETED, result.getTransactionId());
                  kafkaTemplate.send("payment-events", event.getOrderId(), completedEvent);
              } else {
                  payment.setStatus(PaymentStatus.FAILED);
                  payment.setFailureReason(result.getErrorMessage());
                  paymentRepository.save(payment);
                  
                  // Publish payment failed event
                  PaymentFailedEvent failedEvent = new PaymentFailedEvent(
                      event.getOrderId(), PaymentStatus.FAILED, result.getErrorMessage());
                  kafkaTemplate.send("payment-events", event.getOrderId(), failedEvent);
              }
          } catch (Exception e) {
              log.error("Error processing payment", e);
              
              // Publish payment failed event
              PaymentFailedEvent failedEvent = new PaymentFailedEvent(
                  event.getOrderId(), PaymentStatus.FAILED, e.getMessage());
              kafkaTemplate.send("payment-events", event.getOrderId(), failedEvent);
          }
      }
      
      @KafkaListener(topics = "payment-refund-events")
      @Transactional
      public void processRefund(RefundPaymentEvent event) {
          Payment payment = paymentRepository.findByOrderId(event.getOrderId()).orElseThrow();
          
          try {
              // Process refund
              RefundResult result = paymentGateway.refundPayment(payment.getTransactionId(), event.getAmount());
              
              if (result.isSuccessful()) {
                  payment.setStatus(PaymentStatus.REFUNDED);
                  paymentRepository.save(payment);
                  
                  // Publish refund completed event
                  RefundCompletedEvent completedEvent = new RefundCompletedEvent(
                      event.getOrderId(), RefundStatus.COMPLETED);
                  kafkaTemplate.send("refund-events", event.getOrderId(), completedEvent);
              } else {
                  payment.setStatus(PaymentStatus.REFUND_FAILED);
                  payment.setFailureReason(result.getErrorMessage());
                  paymentRepository.save(payment);
                  
                  // Publish refund failed event
                  RefundFailedEvent failedEvent = new RefundFailedEvent(
                      event.getOrderId(), RefundStatus.FAILED, result.getErrorMessage());
                  kafkaTemplate.send("refund-events", event.getOrderId(), failedEvent);
              }
          } catch (Exception e) {
              log.error("Error processing refund", e);
              
              // Publish refund failed event
              RefundFailedEvent failedEvent = new RefundFailedEvent(
                  event.getOrderId(), RefundStatus.FAILED, e.getMessage());
              kafkaTemplate.send("refund-events", event.getOrderId(), failedEvent);
          }
      }
  }
  
  // Inventory Service
  @Service
  public class InventoryService {
      private final KafkaTemplate<String, Object> kafkaTemplate;
      
      @KafkaListener(topics = "order-payment-events")
      @Transactional
      public void reserveInventory(OrderPaymentCompletedEvent event) {
          try {
              boolean allItemsAvailable = true;
              List<InventoryReservation> reservations = new ArrayList<>();
              
              // Check and reserve inventory
              for (OrderItem item : event.getItems()) {
                  Inventory inventory = inventoryRepository.findByProductId(item.getProductId());
                  
                  if (inventory.getQuantity() >= item.getQuantity()) {
                      // Create reservation
                      InventoryReservation reservation = new InventoryReservation(
                          event.getOrderId(), item.getProductId(), item.getQuantity());
                      reservations.add(reservation);
                      
                      // Update inventory
                      inventory.setQuantity(inventory.getQuantity() - item.getQuantity());
                      inventoryRepository.save(inventory);
                  } else {
                      allItemsAvailable = false;
                      break;
                  }
              }
              
              if (allItemsAvailable) {
                  // Save all reservations
                  inventoryReservationRepository.saveAll(reservations);
                  
                  // Publish inventory reserved event
                  InventoryReservedEvent reservedEvent = new InventoryReservedEvent(
                      event.getOrderId(), InventoryStatus.RESERVED);
                  kafkaTemplate.send("inventory-events", event.getOrderId(), reservedEvent);
              } else {
                  // Publish inventory failed event
                  InventoryFailedEvent failedEvent = new InventoryFailedEvent(
                      event.getOrderId(), InventoryStatus.FAILED, "Insufficient inventory");
                  kafkaTemplate.send("inventory-events", event.getOrderId(), failedEvent);
              }
          } catch (Exception e) {
              log.error("Error reserving inventory", e);
              
              // Publish inventory failed event
              InventoryFailedEvent failedEvent = new InventoryFailedEvent(
                  event.getOrderId(), InventoryStatus.FAILED, e.getMessage());
              kafkaTemplate.send("inventory-events", event.getOrderId(), failedEvent);
          }
      }
      
      @KafkaListener(topics = "order-cancelled-events")
      @Transactional
      public void releaseInventory(OrderCancelledEvent event) {
          List<InventoryReservation> reservations = 
              inventoryReservationRepository.findByOrderId(event.getOrderId());
              
          if (!reservations.isEmpty()) {
              for (InventoryReservation reservation : reservations) {
                  Inventory inventory = inventoryRepository.findByProductId(reservation.getProductId());
                  
                  // Return quantity to inventory
                  inventory.setQuantity(inventory.getQuantity() + reservation.getQuantity());
                  inventoryRepository.save(inventory);
              }
              
              // Delete reservations
              inventoryReservationRepository.deleteAll(reservations);
              
              // Publish inventory released event
              InventoryReleasedEvent releasedEvent = new InventoryReleasedEvent(
                  event.getOrderId(), InventoryStatus.RELEASED);
              kafkaTemplate.send("inventory-released-events", event.getOrderId(), releasedEvent);
          }
      }
  }
  ```

- **Orchestration-based Saga**:
  ```java
  // Saga Orchestrator Service
  @Service
  public class OrderSagaOrchestrator {
      private final KafkaTemplate<String, Object> kafkaTemplate;
      private final SagaInstanceRepository sagaRepository;
      
      @KafkaListener(topics = "order-events")
      @Transactional
      public void startOrderSaga(OrderCreatedEvent event) {
          // Create saga instance
          SagaInstance saga = new SagaInstance(
              UUID.randomUUID().toString(),
              "ORDER_SAGA",
              event.getOrderId(),
              SagaStatus.STARTED,
              objectMapper.writeValueAsString(event)
          );
          sagaRepository.save(saga);
          
          // Start payment step
          PaymentCommand paymentCommand = new PaymentCommand(
              event.getOrderId(), event.getCustomerId(), event.getAmount());
          kafkaTemplate.send("payment-commands", event.getOrderId(), paymentCommand);
      }
      
      @KafkaListener(topics = "payment-events")
      @Transactional
      public void handlePaymentResponse(PaymentEvent event) {
          SagaInstance saga = sagaRepository.findByTypeAndTargetId("ORDER_SAGA", event.getOrderId())
              .orElseThrow();
              
          if (event.getStatus() == PaymentStatus.COMPLETED) {
              saga.setCurrentStep("INVENTORY_RESERVATION");
              sagaRepository.save(saga);
              
              // Extract order details from saga data
              OrderCreatedEvent orderEvent = objectMapper.readValue(saga.getSagaData(), OrderCreatedEvent.class);
              
              // Send inventory command
              InventoryReservationCommand inventoryCommand = new InventoryReservationCommand(
                  event.getOrderId(), orderEvent.getItems());
              kafkaTemplate.send("inventory-commands", event.getOrderId(), inventoryCommand);
          } else {
              // Payment failed, end saga
              saga.setStatus(SagaStatus.FAILED);
              saga.setEndReason("Payment failed: " + event.getErrorMessage());
              sagaRepository.save(saga);
              
              // Notify order service
              OrderUpdateCommand orderCommand = new OrderUpdateCommand(
                  event.getOrderId(), OrderStatus.PAYMENT_FAILED, event.getErrorMessage());
              kafkaTemplate.send("order-commands", event.getOrderId(), orderCommand);
          }
      }
      
      @KafkaListener(topics = "inventory-events")
      @Transactional
      public void handleInventoryResponse(InventoryEvent event) {
          SagaInstance saga = sagaRepository.findByTypeAndTargetId("ORDER_SAGA", event.getOrderId())
              .orElseThrow();
              
          if (event.getStatus() == InventoryStatus.RESERVED) {
              // Inventory reserved, complete saga
              saga.setCurrentStep("COMPLETED");
              saga.setStatus(SagaStatus.COMPLETED);
              sagaRepository.save(saga);
              
              // Notify order service
              OrderUpdateCommand orderCommand = new OrderUpdateCommand(
                  event.getOrderId(), OrderStatus.COMPLETED, null);
              kafkaTemplate.send("order-commands", event.getOrderId(), orderCommand);
          } else {
              // Inventory failed, compensate payment
              saga.setCurrentStep("PAYMENT_COMPENSATION");
              saga.setStatus(SagaStatus.COMPENSATING);
              sagaRepository.save(saga);
              
              // Extract order details from saga data
              OrderCreatedEvent orderEvent = objectMapper.readValue(saga.getSagaData(), OrderCreatedEvent.class);
              
              // Send payment refund command
              PaymentRefundCommand refundCommand = new PaymentRefundCommand(
                  event.getOrderId(), orderEvent.getAmount());
              kafkaTemplate.send("payment-refund-commands", event.getOrderId(), refundCommand);
          }
      }
      
      @KafkaListener(topics = "refund-events")
      @Transactional
      public void handleRefundResponse(RefundEvent event) {
          SagaInstance saga = sagaRepository.findByTypeAndTargetId("ORDER_SAGA", event.getOrderId())
              .orElseThrow();
              
          // Mark saga as compensated
          saga.setStatus(SagaStatus.COMPENSATED);
          sagaRepository.save(saga);
          
          // Notify order service
          OrderUpdateCommand orderCommand = new OrderUpdateCommand(
              event.getOrderId(), OrderStatus.CANCELLED, "Inventory reservation failed");
          kafkaTemplate.send("order-commands", event.getOrderId(), orderCommand);
      }
  }
  ```

**2. Outbox Pattern with Change Data Capture (CDC)**:

The Outbox pattern ensures atomicity between database updates and message publishing by storing outgoing messages in a database table as part of the same transaction.

```java
// Order Service with Outbox Pattern
@Service
public class OrderService {
    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    
    @Transactional
    public void createOrder(Order order) {
        // Save order
        Order savedOrder = orderRepository.save(order);
        
        // Create outbox event
        OutboxEvent outboxEvent = new OutboxEvent(
            UUID.randomUUID(),
            "OrderCreated",
            "Order",
            savedOrder.getId().toString(),
            objectMapper.writeValueAsString(savedOrder),
            LocalDateTime.now(),
            false
        );
        
        // Save outbox event in same transaction
        outboxRepository.save(outboxEvent);
    }
}

// Outbox Relay Service
@Service
public class OutboxRelayService {
    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    
    @Scheduled(fixedRate = 1000)
    @Transactional
    public void publishOutboxEvents() {
        List<OutboxEvent> unpublishedEvents = outboxRepository.findByPublishedOrderByCreatedAtAsc(false, PageRequest.of(0, 100));
        
        for (OutboxEvent event : unpublishedEvents) {
            try {
                // Determine topic based on event type
                String topic = determineTopicForEventType(event.getEventType());
                
                // Send to Kafka
                kafkaTemplate.send(topic, event.getAggregateId(), event.getPayload())
                    .get(5, TimeUnit.SECONDS); // Wait for send to complete
                
                // Mark as published
                event.setPublished(true);
                outboxRepository.save(event);
            } catch (Exception e) {
                log.error("Failed to publish outbox event: {}", event.getId(), e);
                // Will retry on next schedule
            }
        }
    }
    
    private String determineTopicForEventType(String eventType) {
        switch (eventType) {
            case "OrderCreated":
                return "order-events";
            case "PaymentProcessed":
                return "payment-events";
            case "InventoryReserved":
                return "inventory-events";
            default:
                return "default-events";
        }
    }
}
```

**3. Two-Phase Commit with Transaction Log Tailing**:

For systems requiring stronger consistency guarantees, you can implement a two-phase commit protocol using Kafka as the transaction coordinator.

```java
// Transaction Coordinator Service
@Service
public class TransactionCoordinatorService {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TransactionRepository transactionRepository;
    
    @Transactional
    public String beginTransaction(String transactionType, Map<String, Object> metadata) {
        String transactionId = UUID.randomUUID().toString();
        
        // Create transaction record
        DistributedTransaction tx = new DistributedTransaction(
            transactionId,
            transactionType,
            TransactionStatus.STARTED,
            objectMapper.writeValueAsString(metadata),
            new ArrayList<>(),
            LocalDateTime.now()
        );
        transactionRepository.save(tx);
        
        // Publish transaction started event
        TransactionStartedEvent event = new TransactionStartedEvent(
            transactionId, transactionType, metadata);
        kafkaTemplate.send("transaction-events", transactionId, event);
        
        return transactionId;
    }
    
    @Transactional
    public void prepareParticipant(String transactionId, String participantId, String participantType) {
        DistributedTransaction tx = transactionRepository.findById(transactionId)
            .orElseThrow(() -> new TransactionNotFoundException(transactionId));
            
        // Add participant
        TransactionParticipant participant = new TransactionParticipant(
            participantId, participantType, ParticipantStatus.PREPARING);
        tx.getParticipants().add(participant);
        transactionRepository.save(tx);
        
        // Send prepare command to participant
        PrepareCommand command = new PrepareCommand(transactionId, participantId);
        kafkaTemplate.send(participantType + "-prepare-commands", participantId, command);
    }
    
    @KafkaListener(topics = {"order-prepare-responses", "payment-prepare-responses", "inventory-prepare-responses"})
    @Transactional
    public void handlePrepareResponse(PrepareResponse response) {
        DistributedTransaction tx = transactionRepository.findById(response.getTransactionId())
            .orElseThrow(() -> new TransactionNotFoundException(response.getTransactionId()));
            
        // Update participant status
        tx.getParticipants().stream()
            .filter(p -> p.getParticipantId().equals(response.getParticipantId()))
            .findFirst()
            .ifPresent(p -> p.setStatus(
                response.isPrepared() ? ParticipantStatus.PREPARED : ParticipantStatus.FAILED));
        transactionRepository.save(tx);
        
        // Check if all participants are prepared
        boolean allPrepared = tx.getParticipants().stream()
            .allMatch(p -> p.getStatus() == ParticipantStatus.PREPARED);
            
        boolean anyFailed = tx.getParticipants().stream()
            .anyMatch(p -> p.getStatus() == ParticipantStatus.FAILED);
            
        if (allPrepared) {
            // All prepared, commit
            commitTransaction(tx);
        } else if (anyFailed) {
            // At least one failed, abort
            abortTransaction(tx);
        }
        // Otherwise, wait for more responses
    }
    
    private void commitTransaction(DistributedTransaction tx) {
        tx.setStatus(TransactionStatus.COMMITTING);
        transactionRepository.save(tx);
        
        // Send commit commands to all participants
        for (TransactionParticipant participant : tx.getParticipants()) {
            CommitCommand command = new CommitCommand(tx.getId(), participant.getParticipantId());
            kafkaTemplate.send(participant.getParticipantType() + "-commit-commands", 
                              participant.getParticipantId(), command);
        }
    }
    
    private void abortTransaction(DistributedTransaction tx) {
        tx.setStatus(TransactionStatus.ABORTING);
        transactionRepository.save(tx);
        
        // Send abort commands to all participants that were prepared
        for (TransactionParticipant participant : tx.getParticipants()) {
            if (participant.getStatus() == ParticipantStatus.PREPARED) {
                AbortCommand command = new AbortCommand(tx.getId(), participant.getParticipantId());
                kafkaTemplate.send(participant.getParticipantType() + "-abort-commands", 
                                  participant.getParticipantId(), command);
            }
        }
    }
    
    @KafkaListener(topics = {"order-commit-responses", "payment-commit-responses", "inventory-commit-responses",
                           "order-abort-responses", "payment-abort-responses", "inventory-abort-responses"})
    @Transactional
    public void handleFinalResponse(FinalResponse response) {
        DistributedTransaction tx = transactionRepository.findById(response.getTransactionId())
            .orElseThrow(() -> new TransactionNotFoundException(response.getTransactionId()));
            
        // Update participant status
        tx.getParticipants().stream()
            .filter(p -> p.getParticipantId().equals(response.getParticipantId()))
            .findFirst()
            .ifPresent(p -> p.setStatus(
                tx.getStatus() == TransactionStatus.COMMITTING ? 
                    ParticipantStatus.COMMITTED : ParticipantStatus.ABORTED));
        
        // Check if all participants have responded
        boolean allFinished = tx.getParticipants().stream()
            .allMatch(p -> p.getStatus() == ParticipantStatus.COMMITTED || 
                          p.getStatus() == ParticipantStatus.ABORTED);
                          
        if (allFinished) {
            // Update transaction status
            tx.setStatus(tx.getStatus() == TransactionStatus.COMMITTING ? 
                        TransactionStatus.COMMITTED : TransactionStatus.ABORTED);
            tx.setCompletedAt(LocalDateTime.now());
            transactionRepository.save(tx);
            
            // Publish transaction completed event
            TransactionCompletedEvent event = new TransactionCompletedEvent(
                tx.getId(), tx.getStatus(), tx.getParticipants());
            kafkaTemplate.send("transaction-completed-events", tx.getId(), event);
        }
    }
}

// Order Service as Transaction Participant
@Service
public class OrderTransactionParticipant {
    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @KafkaListener(topics = "order-prepare-commands")
    @Transactional
    public void handlePrepareCommand(PrepareCommand command) {
        try {
            // Get order
            Order order = orderRepository.findById(command.getParticipantId())
                .orElseThrow();
                
            // Check if order can be prepared
            boolean canPrepare = validateOrderCanBePrepared(order);
            
            if (canPrepare) {
                // Mark order as preparing
                order.setTransactionId(command.getTransactionId());
                order.setStatus(OrderStatus.PREPARING);
                orderRepository.save(order);
                
                // Send prepared response
                PrepareResponse response = new PrepareResponse(
                    command.getTransactionId(), command.getParticipantId(), true);
                kafkaTemplate.send("order-prepare-responses", command.getParticipantId(), response);
            } else {
                // Send failed response
                PrepareResponse response = new PrepareResponse(
                    command.getTransactionId(), command.getParticipantId(), false);
                kafkaTemplate.send("order-prepare-responses", command.getParticipantId(), response);
            }
        } catch (Exception e) {
            log.error("Error preparing order", e);
            
            // Send failed response
            PrepareResponse response = new PrepareResponse(
                command.getTransactionId(), command.getParticipantId(), false);
            kafkaTemplate.send("order-prepare-responses", command.getParticipantId(), response);
        }
    }
    
    @KafkaListener(topics = "order-commit-commands")
    @Transactional
    public void handleCommitCommand(CommitCommand command) {
        try {
            // Get order
            Order order = orderRepository.findById(command.getParticipantId())
                .orElseThrow();
                
            // Commit order
            order.setStatus(OrderStatus.CREATED);
            orderRepository.save(order);
            
            // Send committed response
            CommitResponse response = new CommitResponse(
                command.getTransactionId(), command.getParticipantId(), true);
            kafkaTemplate.send("order-commit-responses", command.getParticipantId(), response);
        } catch (Exception e) {
            log.error("Error committing order", e);
            
            // Send failed response
            CommitResponse response = new CommitResponse(
                command.getTransactionId(), command.getParticipantId(), false);
            kafkaTemplate.send("order-commit-responses", command.getParticipantId(), response);
        }
    }
    
    @KafkaListener(topics = "order-abort-commands")
    @Transactional
    public void handleAbortCommand(AbortCommand command) {
        try {
            // Get order
            Order order = orderRepository.findById(command.getParticipantId())
                .orElseThrow();
                
            // Abort order
            order.setStatus(OrderStatus.CANCELLED);
            order.setTransactionId(null);
            orderRepository.save(order);
            
            // Send aborted response
            AbortResponse response = new AbortResponse(
                command.getTransactionId(), command.getParticipantId(), true);
            kafkaTemplate.send("order-abort-responses", command.getParticipantId(), response);
        } catch (Exception e) {
            log.error("Error aborting order", e);
            
            // Send failed response
            AbortResponse response = new AbortResponse(
                command.getTransactionId(), command.getParticipantId(), false);
            kafkaTemplate.send("order-abort-responses", command.getParticipantId(), response);
        }
    }
}
```

**4. Event Sourcing with Compensating Actions**:

Event sourcing stores all state changes as a sequence of events, making it easier to implement compensating actions for failed transactions.

```java
// Order Aggregate
@Service
public class OrderAggregate {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EventStoreRepository eventStore;
    
    @Transactional
    public String createOrder(CreateOrderCommand command) {
        String orderId = UUID.randomUUID().toString();
        
        // Create order created event
        OrderCreatedEvent event = new OrderCreatedEvent(
            orderId,
            command.getCustomerId(),
            command.getItems(),
            command.getAmount(),
            OrderStatus.CREATED,
            LocalDateTime.now()
        );
        
        // Store event
        storeAndPublishEvent(orderId, event);
        
        return orderId;
    }
    
    @KafkaListener(topics = "payment-events")
    @Transactional
    public void handlePaymentEvent(PaymentEvent paymentEvent) {
        String orderId = paymentEvent.getOrderId();
        
        // Create order payment event
        OrderPaymentEvent event;
        
        if (paymentEvent.getStatus() == PaymentStatus.COMPLETED) {
            event = new OrderPaymentEvent(
                orderId,
                PaymentStatus.COMPLETED,
                paymentEvent.getTransactionId(),
                LocalDateTime.now()
            );
        } else {
            event = new OrderPaymentEvent(
                orderId,
                PaymentStatus.FAILED,
                null,
                LocalDateTime.now()
            );
        }
        
        // Store and publish event
        storeAndPublishEvent(orderId, event);
    }
    
    @KafkaListener(topics = "inventory-events")
    @Transactional
    public void handleInventoryEvent(InventoryEvent inventoryEvent) {
        String orderId = inventoryEvent.getOrderId();
        
        // Create order inventory event
        OrderInventoryEvent event;
        
        if (inventoryEvent.getStatus() == InventoryStatus.RESERVED) {
            event = new OrderInventoryEvent(
                orderId,
                InventoryStatus.RESERVED,
                LocalDateTime.now()
            );
        } else {
            event = new OrderInventoryEvent(
                orderId,
                InventoryStatus.FAILED,
                LocalDateTime.now()
            );
            
            // If inventory failed, create compensation event
            OrderCompensationEvent compensationEvent = new OrderCompensationEvent(
                orderId,
                "Inventory reservation failed",
                LocalDateTime.now()
            );
            
            // Store and publish compensation event
            storeAndPublishEvent(orderId, compensationEvent);
        }
        
        // Store and publish inventory event
        storeAndPublishEvent(orderId, event);
    }
    
    private void storeAndPublishEvent(String aggregateId, Object event) {
        // Store event in event store
        EventRecord record = new EventRecord(
            UUID.randomUUID().toString(),
            aggregateId,
            event.getClass().getSimpleName(),
            objectMapper.writeValueAsString(event),
            LocalDateTime.now()
        );
        eventStore.save(record);
        
        // Publish event to Kafka
        String topic = determineTopicForEvent(event);
        kafkaTemplate.send(topic, aggregateId, event);
    }
    
    private String determineTopicForEvent(Object event) {
        if (event instanceof OrderCreatedEvent) {
            return "order-created-events";
        } else if (event instanceof OrderPaymentEvent) {
            return "order-payment-events";
        } else if (event instanceof OrderInventoryEvent) {
            return "order-inventory-events";
        } else if (event instanceof OrderCompensationEvent) {
            return "order-compensation-events";
        } else {
            return "order-events";
        }
    }
    
    // Query side - rebuild order state from events
    public Order getOrder(String orderId) {
        List<EventRecord> events = eventStore.findByAggregateIdOrderByCreatedAt(orderId);
        
        if (events.isEmpty()) {
            throw new OrderNotFoundException(orderId);
        }
        
        Order order = new Order();
        order.setId(orderId);
        
        for (EventRecord record : events) {
            applyEvent(order, record);
        }
        
        return order;
    }
    
    private void applyEvent(Order order, EventRecord record) {
        switch (record.getEventType()) {
            case "OrderCreatedEvent":
                OrderCreatedEvent createdEvent = objectMapper.readValue(record.getPayload(), OrderCreatedEvent.class);
                order.setCustomerId(createdEvent.getCustomerId());
                order.setItems(createdEvent.getItems());
                order.setAmount(createdEvent.getAmount());
                order.setStatus(createdEvent.getStatus());
                break;
                
            case "OrderPaymentEvent":
                OrderPaymentEvent paymentEvent = objectMapper.readValue(record.getPayload(), OrderPaymentEvent.class);
                if (paymentEvent.getStatus() == PaymentStatus.COMPLETED) {
                    order.setStatus(OrderStatus.PAYMENT_COMPLETED);
                    order.setPaymentTransactionId(paymentEvent.getTransactionId());
                } else {
                    order.setStatus(OrderStatus.PAYMENT_FAILED);
                }
                break;
                
            case "OrderInventoryEvent":
                OrderInventoryEvent inventoryEvent = objectMapper.readValue(record.getPayload(), OrderInventoryEvent.class);
                if (inventoryEvent.getStatus() == InventoryStatus.RESERVED) {
                    order.setStatus(OrderStatus.INVENTORY_RESERVED);
                } else {
                    order.setStatus(OrderStatus.INVENTORY_FAILED);
                }
                break;
                
            case "OrderCompensationEvent":
                OrderCompensationEvent compensationEvent = objectMapper.readValue(record.getPayload(), OrderCompensationEvent.class);
                order.setStatus(OrderStatus.CANCELLED);
                order.setCancellationReason(compensationEvent.getReason());
                break;
        }
    }
}
```

**5. Hybrid Approach with Stateful Stream Processing**:

Using Kafka Streams for transaction coordination provides a stateful, fault-tolerant approach.

```java
@Component
public class OrderTransactionProcessor {
    @Bean
    public KStream<String, Object> processOrderTransactions(StreamsBuilder builder) {
        // Input topics
        KStream<String, OrderCreatedEvent> orderStream = 
            builder.stream("order-created-events", 
                         Consumed.with(Serdes.String(), orderEventSerde));
                         
        KStream<String, PaymentEvent> paymentStream = 
            builder.stream("payment-events", 
                         Consumed.with(Serdes.String(), paymentEventSerde));
                         
        KStream<String, InventoryEvent> inventoryStream = 
            builder.stream("inventory-events", 
                         Consumed.with(Serdes.String(), inventoryEventSerde));
        
        // State store for tracking transaction status
        StoreBuilder<KeyValueStore<String, TransactionStatus>> storeBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("transaction-store"),
                Serdes.String(),
                transactionStatusSerde
            );
        builder.addStateStore(storeBuilder);
        
        // Process order created events
        KStream<String, TransactionCommand> paymentCommands = orderStream
            .transform(() -> new TransformerSupplier<String, OrderCreatedEvent, KeyValue<String, TransactionCommand>>() {
                @Override
                public Transformer<String, OrderCreatedEvent, KeyValue<String, TransactionCommand>> get() {
                    return new Transformer<String, OrderCreatedEvent, KeyValue<String, TransactionCommand>>() {
                        private KeyValueStore<String, TransactionStatus> stateStore;
                        
                        @Override
                        public void init(ProcessorContext context) {
                            this.stateStore = (KeyValueStore<String, TransactionStatus>) context
                                .getStateStore("transaction-store");
                        }
                        
                        @Override
                        public KeyValue<String, TransactionCommand> transform(String key, OrderCreatedEvent value) {
                            // Initialize transaction
                            TransactionStatus status = new TransactionStatus(
                                key, 
                                "ORDER_TRANSACTION",
                                TransactionStep.PAYMENT_PENDING,
                                null
                            );
                            stateStore.put(key, status);
                            
                            // Create payment command
                            PaymentCommand command = new PaymentCommand(
                                key, value.getCustomerId(), value.getAmount());
                                
                            return KeyValue.pair(key, command);
                        }
                        
                        @Override
                        public void close() {}
                    };
                }
            }, "transaction-store")
            .to("payment-commands");
            
        // Process payment events
        KStream<String, TransactionCommand> inventoryCommands = paymentStream
            .transform(() -> new TransformerSupplier<String, PaymentEvent, KeyValue<String, TransactionCommand>>() {
                @Override
                public Transformer<String, PaymentEvent, KeyValue<String, TransactionCommand>> get() {
                    return new Transformer<String, PaymentEvent, KeyValue<String, TransactionCommand>>() {
                        private KeyValueStore<String, TransactionStatus> stateStore;
                        
                        @Override
                        public void init(ProcessorContext context) {
                            this.stateStore = (KeyValueStore<String, TransactionStatus>) context
                                .getStateStore("transaction-store");
                        }
                        
                        @Override
                        public KeyValue<String, TransactionCommand> transform(String key, PaymentEvent value) {
                            TransactionStatus status = stateStore.get(key);
                            
                            if (status == null || status.getStep() != TransactionStep.PAYMENT_PENDING) {
                                // Ignore event if not in correct state
                                return null;
                            }
                            
                            if (value.getStatus() == PaymentStatus.COMPLETED) {
                                // Update transaction status
                                status.setStep(TransactionStep.INVENTORY_PENDING);
                                stateStore.put(key, status);
                                
                                // Get order details from state
                                Order order = getOrderDetails(key);
                                
                                // Create inventory command
                                InventoryCommand command = new InventoryCommand(
                                    key, order.getItems());
                                    
                                return KeyValue.pair(key, command);
                            } else {
                                // Payment failed, mark transaction as failed
                                status.setStep(TransactionStep.FAILED);
                                status.setFailureReason("Payment failed: " + value.getErrorMessage());
                                stateStore.put(key, status);
                                
                                // Create order update command
                                OrderUpdateCommand command = new OrderUpdateCommand(
                                    key, OrderStatus.PAYMENT_FAILED, value.getErrorMessage());
                                    
                                return KeyValue.pair(key, command);
                            }
                        }
                        
                        @Override
                        public void close() {}
                    };
                }
            }, "transaction-store");
            
        // Route commands based on type
        inventoryCommands
            .filter((k, v) -> v instanceof InventoryCommand)
            .to("inventory-commands");
            
        inventoryCommands
            .filter((k, v) -> v instanceof OrderUpdateCommand)
            .to("order-update-commands");
            
        // Process inventory events
        KStream<String, TransactionCommand> finalCommands = inventoryStream
            .transform(() -> new TransformerSupplier<String, InventoryEvent, KeyValue<String, TransactionCommand>>() {
                @Override
                public Transformer<String, InventoryEvent, KeyValue<String, TransactionCommand>> get() {
                    return new Transformer<String, InventoryEvent, KeyValue<String, TransactionCommand>>() {
                        private KeyValueStore<String, TransactionStatus> stateStore;
                        
                        @Override
                        public void init(ProcessorContext context) {
                            this.stateStore = (KeyValueStore<String, TransactionStatus>) context
                                .getStateStore("transaction-store");
                        }
                        
                        @Override
                        public KeyValue<String, TransactionCommand> transform(String key, InventoryEvent value) {
                            TransactionStatus status = stateStore.get(key);
                            
                            if (status == null || status.getStep() != TransactionStep.INVENTORY_PENDING) {
                                // Ignore event if not in correct state
                                return null;
                            }
                            
                            if (value.getStatus() == InventoryStatus.RESERVED) {
                                // Update transaction status
                                status.setStep(TransactionStep.COMPLETED);
                                stateStore.put(key, status);
                                
                                // Create order update command
                                OrderUpdateCommand command = new OrderUpdateCommand(
                                    key, OrderStatus.COMPLETED, null);
                                    
                                return KeyValue.pair(key, command);
                            } else {
                                // Inventory failed, need to compensate payment
                                status.setStep(TransactionStep.COMPENSATING_PAYMENT);
                                stateStore.put(key, status);
                                
                                // Create payment refund command
                                Order order = getOrderDetails(key);
                                PaymentRefundCommand command = new PaymentRefundCommand(
                                    key, order.getAmount());
                                    
                                return KeyValue.pair(key, command);
                            }
                        }
                        
                        @Override
                        public void close() {}
                    };
                }
            }, "transaction-store");
            
        // Route final commands
        finalCommands
            .filter((k, v) -> v instanceof OrderUpdateCommand)
            .to("order-update-commands");
            
        finalCommands
            .filter((k, v) -> v instanceof PaymentRefundCommand)
            .to("payment-refund-commands");
            
        return finalCommands;
    }
    
    private Order getOrderDetails(String orderId) {
        // In a real implementation, this would retrieve order details from a state store
        // or external service
        return new Order(); // Placeholder
    }
}
```

**Comparison of Approaches**:

1. **Saga Pattern**:
   - **Pros**: Simple to implement, scales well, clear separation of concerns
   - **Cons**: Complex compensation logic, eventual consistency only
   - **Best for**: Most microservice architectures, especially when services are owned by different teams

2. **Outbox Pattern with CDC**:
   - **Pros**: Ensures atomicity between database and messaging, reliable
   - **Cons**: Requires CDC infrastructure, adds complexity
   - **Best for**: Systems where database-messaging consistency is critical

3. **Two-Phase Commit**:
   - **Pros**: Strong consistency guarantees
   - **Cons**: Complex, potential for blocking, performance impact
   - **Best for**: Financial systems or where strong consistency is required

4. **Event Sourcing**:
   - **Pros**: Complete audit trail, easy to implement compensating actions
   - **Cons**: Learning curve, eventual consistency, query complexity
   - **Best for**: Systems requiring complete history and audit trails

5. **Stateful Stream Processing**:
   - **Pros**: Fault-tolerant, scalable, handles backpressure well
   - **Cons**: Complex to implement and debug
   - **Best for**: High-throughput systems with complex transaction flows

**Best Practices**:

1. **Idempotency**: All services should be idempotent to handle duplicate messages
2. **Unique Transaction IDs**: Use UUIDs for tracking transactions across services
3. **Timeout Handling**: Implement timeouts for long-running transactions
4. **Monitoring**: Track transaction status and implement alerting for stuck transactions
5. **Dead Letter Queues**: Capture failed messages for manual intervention
6. **Compensating Transactions**: Design clear compensation paths for failures
7. **Transaction Logs**: Maintain detailed logs of all transaction steps
8. **Correlation IDs**: Use correlation IDs for tracing transactions across services
9. **Circuit Breakers**: Implement circuit breakers to prevent cascading failures
10. **Retry Policies**: Define clear retry policies with backoff strategies

By carefully selecting and implementing the appropriate pattern based on your consistency requirements, you can build reliable distributed transactions across microservices using Kafka as the backbone.

## Real-World Scenarios and Best Practices

### Q16: Describe a complex Kafka-based architecture you've designed or worked with. What were the challenges and how did you overcome them?
**Answer:** I've designed and implemented a real-time data processing platform for a financial services company that handled millions of transactions per day. The architecture needed to process transactions, detect fraud, update customer profiles, and generate real-time analytics while ensuring data consistency, fault tolerance, and regulatory compliance.

**Architecture Overview**:

1. **Data Ingestion Layer**:
   - Multiple source systems (payment gateways, mobile apps, web applications)
   - Kafka Connect for CDC from legacy databases
   - REST APIs with Kafka producers for real-time events
   - Batch imports for historical data

2. **Core Processing Layer**:
   - Kafka Streams applications for transaction enrichment and validation
   - Stateful processing for fraud detection using windowed operations
   - Event-driven microservices for business logic
   - CQRS pattern for separating read and write operations

3. **Storage Layer**:
   - Kafka as the central event log
   - Elasticsearch for search and analytics
   - PostgreSQL for transactional data
   - Redis for caching and rate limiting

4. **Analytics Layer**:
   - ksqlDB for real-time analytics
   - Kafka Connect sinks to data warehouse
   - Custom Kafka consumers for specialized analytics

5. **Monitoring and Operations**:
   - Prometheus and Grafana for metrics
   - Distributed tracing with Jaeger
   - Centralized logging with ELK stack
   - Custom alerting system

**Key Challenges and Solutions**:

1. **Challenge**: Ensuring exactly-once processing for financial transactions
   
   **Solution**:
   - Implemented Kafka's transactions API for exactly-once semantics
   - Used idempotent producers and consumers
   - Designed a two-phase commit protocol for critical operations
   - Implemented the outbox pattern for database-to-Kafka consistency
   - Created a reconciliation system to detect and fix inconsistencies

2. **Challenge**: Handling high throughput (10,000+ messages per second) with low latency
   
   **Solution**:
   - Optimized Kafka cluster with proper hardware (NVMe SSDs, high memory)
   - Tuned producer and consumer configurations for throughput
   - Implemented batching strategies for both producers and consumers
   - Used compression (lz4) to reduce network and storage requirements
   - Designed efficient partitioning strategies based on transaction IDs
   - Implemented backpressure mechanisms to handle traffic spikes
   - Scaled horizontally with consumer groups and Kafka Streams instances

3. **Challenge**: Ensuring data consistency across multiple services and databases
   
   **Solution**:
   - Implemented the Saga pattern for distributed transactions
   - Used event sourcing for critical domains
   - Created a dedicated "consistency service" to monitor and fix inconsistencies
   - Implemented compensating transactions for failure scenarios
   - Used change data capture (CDC) to synchronize databases
   - Designed a robust retry mechanism with exponential backoff

4. **Challenge**: Meeting regulatory requirements for data retention and auditability
   
   **Solution**:
   - Configured multi-tiered storage for Kafka (hot/warm/cold)
   - Implemented topic-specific retention policies
   - Created an immutable audit log using compacted topics
   - Designed a comprehensive data lineage tracking system
   - Implemented point-in-time recovery capabilities
   - Created data masking for sensitive information

5. **Challenge**: Managing schema evolution without downtime
   
   **Solution**:
   - Implemented Confluent Schema Registry with Avro serialization
   - Enforced backward and forward compatibility
   - Created a schema review process for breaking changes
   - Implemented versioned APIs and consumers
   - Designed a blue-green deployment strategy for schema migrations
   - Created schema validation tests in CI/CD pipeline

6. **Challenge**: Debugging and troubleshooting in a distributed system
   
   **Solution**:
   - Implemented correlation IDs across all services
   - Created a centralized transaction tracking service
   - Designed comprehensive logging standards
   - Implemented distributed tracing with OpenTracing
   - Created custom monitoring dashboards for each service
   - Developed a "message journey" visualization tool
   - Implemented dead letter queues with retry capabilities

7. **Challenge**: Handling failure scenarios and ensuring resilience
   
   **Solution**:
   - Designed circuit breakers for external dependencies
   - Implemented graceful degradation strategies
   - Created automated recovery procedures
   - Designed active-active multi-region deployment
   - Implemented chaos engineering practices
   - Conducted regular disaster recovery drills
   - Created a "failure mode effects analysis" for each component

8. **Challenge**: Managing a large and complex Kafka ecosystem
   
   **Solution**:
   - Implemented infrastructure as code for all components
   - Created a self-service platform for topic creation and management
   - Designed standardized monitoring and alerting
   - Implemented automated topic rebalancing
   - Created a comprehensive documentation system
   - Established a Kafka center of excellence
   - Implemented automated testing for Kafka applications

**Technical Implementation Details**:

1. **Fraud Detection Pipeline**:
   ```java
   // Kafka Streams topology for real-time fraud detection
   StreamsBuilder builder = new StreamsBuilder();
   
   // Read transaction events
   KStream<String, Transaction> transactions = builder.stream(
       "transaction-events",
       Consumed.with(Serdes.String(), transactionSerde)
   );
   
   // Enrich transactions with customer data
   KStream<String, EnrichedTransaction> enrichedTransactions = transactions
       .selectKey((k, v) -> v.getCustomerId())
       .join(
           builder.globalTable("customer-profiles", 
                             Consumed.with(Serdes.String(), customerProfileSerde)),
           (customerId, transaction, customerProfile) -> 
               new EnrichedTransaction(transaction, customerProfile)
       )
       .selectKey((k, v) -> v.getTransaction().getTransactionId());
   
   // Calculate velocity metrics (transactions per minute)
   KTable<Windowed<String>, Long> velocityByCustomer = enrichedTransactions
       .selectKey((k, v) -> v.getCustomerProfile().getCustomerId())
       .groupByKey()
       .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
       .count();
   
   // Join velocity metrics back to transactions
   KStream<String, TransactionWithRisk> transactionsWithVelocity = enrichedTransactions
       .join(
           velocityByCustomer.toStream()
               .map((windowed, count) -> KeyValue.pair(windowed.key(), count)),
           (enrichedTx, velocity) -> 
               new TransactionWithRisk(enrichedTx, velocity)
       );
   
   // Apply fraud detection rules
   KStream<String, ScoredTransaction> scoredTransactions = transactionsWithVelocity
       .mapValues(txWithRisk -> {
           double riskScore = fraudDetectionService.calculateRiskScore(txWithRisk);
           return new ScoredTransaction(txWithRisk.getEnrichedTransaction(), riskScore);
       });
   
   // Split stream based on risk score
   KStream<String, ScoredTransaction>[] branches = scoredTransactions
       .branch(
           (k, v) -> v.getRiskScore() >= 0.8, // High risk
           (k, v) -> v.getRiskScore() >= 0.5, // Medium risk
           (k, v) -> true                     // Low risk
       );
   
   // Process high-risk transactions
   branches[0]
       .mapValues(tx -> {
           tx.setDecision(TransactionDecision.BLOCKED);
           return tx;
       })
       .to("blocked-transactions");
   
   // Process medium-risk transactions
   branches[1]
       .mapValues(tx -> {
           tx.setDecision(TransactionDecision.REVIEW);
           return tx;
       })
       .to("review-transactions");
   
   // Process low-risk transactions
   branches[2]
       .mapValues(tx -> {
           tx.setDecision(TransactionDecision.APPROVED);
           return tx;
       })
       .to("approved-transactions");
   ```

2. **Resilient Consumer Implementation**:
   ```java
   @Service
   public class ResilientTransactionConsumer {
       private final KafkaConsumer<String, Transaction> consumer;
       private final TransactionProcessor processor;
       private final DeadLetterService deadLetterService;
       private final MetricsService metricsService;
       private final CircuitBreaker databaseCircuitBreaker;
       
       @PostConstruct
       public void startConsuming() {
           ExecutorService executor = Executors.newSingleThreadExecutor();
           executor.submit(() -> {
               consumer.subscribe(Collections.singletonList("transaction-events"));
               
               while (true) {
                   try {
                       ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(100));
                       
                       if (!records.isEmpty()) {
                           processRecordBatch(records);
                       }
                   } catch (Exception e) {
                       log.error("Error in consumer loop", e);
                       metricsService.incrementCounter("consumer.error");
                       // Continue processing after error
                   }
               }
           });
       }
       
       private void processRecordBatch(ConsumerRecords<String, Transaction> records) {
           Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
           
           for (TopicPartition partition : records.partitions()) {
               List<ConsumerRecord<String, Transaction>> partitionRecords = records.records(partition);
               processPartitionRecords(partition, partitionRecords, offsetsToCommit);
           }
           
           if (!offsetsToCommit.isEmpty()) {
               consumer.commitSync(offsetsToCommit);
           }
       }
       
       private void processPartitionRecords(
               TopicPartition partition, 
               List<ConsumerRecord<String, Transaction>> records,
               Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
           
           long lastSuccessfulOffset = -1;
           
           for (ConsumerRecord<String, Transaction> record : records) {
               try {
                   processRecord(record);
                   lastSuccessfulOffset = record.offset();
               } catch (RetryableException e) {
                   // Stop processing this partition and retry from this point
                   log.warn("Retryable exception, will retry from offset {}", record.offset(), e);
                   break;
               } catch (Exception e) {
                   // Non-retryable exception, send to DLQ and continue
                   log.error("Failed to process record", e);
                   deadLetterService.sendToDLQ(record, e);
                   lastSuccessfulOffset = record.offset();
               }
           }
           
           if (lastSuccessfulOffset >= 0) {
               offsetsToCommit.put(
                   partition, 
                   new OffsetAndMetadata(lastSuccessfulOffset + 1)
               );
           }
       }
       
       private void processRecord(ConsumerRecord<String, Transaction> record) {
           Transaction transaction = record.value();
           String transactionId = transaction.getTransactionId();
           
           // Add correlation ID for tracing
           MDC.put("correlationId", transactionId);
           MDC.put("partition", String.valueOf(record.partition()));
           MDC.put("offset", String.valueOf(record.offset()));
           
           try {
               // Record metrics
               metricsService.recordLatency("transaction.lag", 
                                          System.currentTimeMillis() - record.timestamp());
               
               // Process with circuit breaker for database operations
               databaseCircuitBreaker.executeCheckedSupplier(() -> {
                   processor.processTransaction(transaction);
                   return null;
               });
               
               // Record success
               metricsService.incrementCounter("transaction.processed");
           } finally {
               MDC.clear();
           }
       }
   }
   ```

3. **Multi-Region Replication Setup**:
   ```java
   @Configuration
   public class MultiRegionReplicationConfig {
       @Bean
       public MirrorMakerConnector primaryToSecondaryConnector() {
           Map<String, String> connectorConfig = new HashMap<>();
           
           // Basic connector configuration
           connectorConfig.put("name", "primary-to-secondary-replication");
           connectorConfig.put("connector.class", "org.apache.kafka.connect.mirror.MirrorSourceConnector");
           
           // Connection settings
           connectorConfig.put("source.cluster.alias", "primary");
           connectorConfig.put("target.cluster.alias", "secondary");
           connectorConfig.put("source.cluster.bootstrap.servers", "primary-kafka:9092");
           connectorConfig.put("target.cluster.bootstrap.servers", "secondary-kafka:9092");
           
           // Authentication settings
           connectorConfig.put("source.cluster.security.protocol", "SSL");
           connectorConfig.put("target.cluster.security.protocol", "SSL");
           connectorConfig.put("source.cluster.ssl.truststore.location", "/etc/kafka/secrets/primary-truststore.jks");
           connectorConfig.put("target.cluster.ssl.truststore.location", "/etc/kafka/secrets/secondary-truststore.jks");
           connectorConfig.put("source.cluster.ssl.keystore.location", "/etc/kafka/secrets/primary-keystore.jks");
           connectorConfig.put("target.cluster.ssl.keystore.location", "/etc/kafka/secrets/secondary-keystore.jks");
           
           // Replication policy
           connectorConfig.put("replication.policy.class", "org.apache.kafka.connect.mirror.IdentityReplicationPolicy");
           connectorConfig.put("replication.factor", "3");
           
           // Topic selection
           connectorConfig.put("topics", "transaction-events,customer-events,payment-events");
           connectorConfig.put("topics.exclude", ".*\.internal,.*\.heartbeat");
           
           // Performance tuning
           connectorConfig.put("tasks.max", "10");
           connectorConfig.put("refresh.topics.interval.seconds", "60");
           connectorConfig.put("sync.topic.configs.enabled", "true");
           connectorConfig.put("sync.topic.acls.enabled", "true");
           
           // Monitoring
           connectorConfig.put("emit.heartbeats.enabled", "true");
           connectorConfig.put("emit.checkpoints.enabled", "true");
           
           return new MirrorMakerConnector(connectorConfig);
       }
       
       @Bean
       public MirrorMakerFailoverDetector failoverDetector() {
           return new MirrorMakerFailoverDetector(
               Arrays.asList("primary-kafka:9092", "secondary-kafka:9092"),
               "heartbeat",
               Duration.ofSeconds(30),
               this::handleFailover
           );
       }
       
       private void handleFailover(String failedCluster, String activeCluster) {
           log.warn("Detected failover from {} to {}", failedCluster, activeCluster);
           
           // Update service discovery
           serviceDiscoveryClient.updateKafkaEndpoint(activeCluster);
           
           // Notify operations team
           alertService.sendAlert(
               "Kafka Failover",
               String.format("Cluster %s failed, failing over to %s", failedCluster, activeCluster)
           );
           
           // Trigger application reconnection
           applicationEventPublisher.publishEvent(new KafkaFailoverEvent(failedCluster, activeCluster));
       }
   }
   ```

4. **Schema Evolution Management**:
   ```java
   @Service
   public class SchemaEvolutionService {
       private final SchemaRegistryClient schemaRegistryClient;
       private final KafkaAdminClient adminClient;
       private final NotificationService notificationService;
       
       public void registerNewSchema(String subject, String schemaDefinition, boolean isBackwardCompatible) 
               throws SchemaRegistrationException {
           try {
               // Parse and validate schema
               Schema schema = new Schema.Parser().parse(schemaDefinition);
               
               // Check if subject exists
               if (schemaRegistryClient.getAllSubjects().contains(subject)) {
                   // Get latest schema
                   SchemaMetadata latestSchema = schemaRegistryClient.getLatestSchemaMetadata(subject);
                   Schema existingSchema = new Schema.Parser().parse(latestSchema.getSchema());
                   
                   // Check compatibility
                   if (isBackwardCompatible) {
                       SchemaCompatibility.SchemaPairCompatibility compatibility = 
                           SchemaCompatibility.checkReaderWriterCompatibility(
                               existingSchema, schema);
                               
                       if (compatibility.getType() != SchemaCompatibilityType.COMPATIBLE) {
                           throw new SchemaRegistrationException(
                               "Schema is not backward compatible: " + 
                               compatibility.getDescription());
                       }
                   }
               }
               
               // Register schema
               int id = schemaRegistryClient.register(subject, schema);
               log.info("Registered new schema for subject {} with id {}", subject, id);
               
               // Notify stakeholders
               notificationService.notifySchemaUpdate(subject, id, schemaDefinition);
               
               // If this is a breaking change, schedule migration
               if (!isBackwardCompatible) {
                   scheduleMigration(subject, id);
               }
           } catch (Exception e) {
               throw new SchemaRegistrationException("Failed to register schema", e);
           }
       }
       
       private void scheduleMigration(String subject, int schemaId) {
           // Extract topic name from subject (remove -value or -key suffix)
           String topicName = subject.replaceAll("-(value|key)$", "");
           
           try {
               // Create migration topic
               String migrationTopic = topicName + "-migration";
               CreateTopicsResult result = adminClient.createTopics(
                   Collections.singleton(
                       new NewTopic(migrationTopic, 
                                  Optional.empty(), 
                                  Optional.empty())
                   )
               );
               result.all().get(30, TimeUnit.SECONDS);
               
               // Schedule migration job
               migrationScheduler.scheduleMigration(
                   new MigrationJob(topicName, migrationTopic, schemaId)
               );
               
               log.info("Scheduled migration for topic {} with new schema id {}", 
                       topicName, schemaId);
           } catch (Exception e) {
               log.error("Failed to schedule migration for topic {}", topicName, e);
               notificationService.notifyMigrationFailure(topicName, schemaId, e.getMessage());
           }
       }
       
       public void migrateData(MigrationJob job) {
           String sourceTopic = job.getSourceTopic();
           String targetTopic = job.getTargetTopic();
           int newSchemaId = job.getSchemaId();
           
           log.info("Starting data migration from {} to {} with schema id {}", 
                   sourceTopic, targetTopic, newSchemaId);
           
           try {
               // Create Kafka Streams topology for migration
               StreamsBuilder builder = new StreamsBuilder();
               
               // Get schemas
               SchemaMetadata newSchemaMetadata = 
                   schemaRegistryClient.getSchemaById(newSchemaId);
               Schema newSchema = new Schema.Parser().parse(newSchemaMetadata.getSchema());
               
               // Create serdes with schema conversion
               Serde<GenericRecord> sourceSerde = createSourceSerde(sourceTopic);
               Serde<GenericRecord> targetSerde = createTargetSerde(targetTopic, newSchema);
               
               // Create migration stream
               builder.stream(sourceTopic, Consumed.with(Serdes.String(), sourceSerde))
                   .mapValues(sourceRecord -> convertRecord(sourceRecord, newSchema))
                   .to(targetTopic, Produced.with(Serdes.String(), targetSerde));
               
               // Start migration
               KafkaStreams streams = new KafkaStreams(
                   builder.build(), 
                   getStreamsConfig(sourceTopic + "-migration")
               );
               
               streams.setUncaughtExceptionHandler((thread, throwable) -> {
                   log.error("Error in migration stream", throwable);
                   notificationService.notifyMigrationError(
                       sourceTopic, targetTopic, throwable.getMessage());
               });
               
               streams.setStateListener((newState, oldState) -> {
                   log.info("Migration stream state changed from {} to {}", 
                           oldState, newState);
                   
                   if (newState == KafkaStreams.State.ERROR) {
                       notificationService.notifyMigrationStateChange(
                           sourceTopic, targetTopic, "ERROR");
                   }
               });
               
               // Start the migration
               streams.start();
               
               // Register shutdown hook
               Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
               
               // Monitor progress
               monitorMigrationProgress(sourceTopic, targetTopic, streams);
           } catch (Exception e) {
               log.error("Failed to start migration", e);
               notificationService.notifyMigrationFailure(
                   sourceTopic, targetTopic, e.getMessage());
           }
       }
       
       private GenericRecord convertRecord(GenericRecord sourceRecord, Schema targetSchema) {
           GenericRecord targetRecord = new GenericData.Record(targetSchema);
           
           // Copy fields from source to target where names match
           for (Schema.Field field : targetSchema.getFields()) {
               String fieldName = field.name();
               
               if (sourceRecord.getSchema().getField(fieldName) != null) {
                   targetRecord.put(fieldName, sourceRecord.get(fieldName));
               } else if (field.defaultVal() != null) {
                   // Use default value if field doesn't exist in source
                   targetRecord.put(fieldName, field.defaultVal());
               } else {
                   // Apply custom conversion logic for new fields
                   targetRecord.put(fieldName, deriveFieldValue(fieldName, sourceRecord));
               }
           }
           
           return targetRecord;
       }
   }
   ```

**Lessons Learned**:

1. **Start with a Clear Data Model**: Define your event schema carefully from the beginning. Changing it later is much harder.

2. **Design for Failure**: Assume every component will fail and design accordingly. This mindset leads to more resilient systems.

3. **Monitoring is Critical**: Invest heavily in monitoring, alerting, and observability. You can't fix what you can't see.

4. **Test at Scale**: Performance in development often doesn't match production. Test with realistic volumes and patterns.

5. **Manage State Carefully**: Stateful processing in Kafka Streams requires careful design, especially for scaling and rebalancing.

6. **Document Everything**: Create comprehensive documentation for topics, schemas, and processing logic. It's invaluable during incidents.

7. **Automate Operations**: Build tools for common operations like topic creation, consumer group management, and offset manipulation.

8. **Implement Gradual Rollouts**: Use techniques like mirrored topics and dual publishing for safe schema and application upgrades.

9. **Balance Consistency and Availability**: Understand the trade-offs and choose appropriate consistency models for each use case.

10. **Build Expertise Incrementally**: Start with simpler patterns and gradually adopt more advanced features as the team gains experience.

This architecture successfully processed millions of financial transactions daily with sub-second latency, maintained 99.99% uptime, and provided real-time insights for fraud detection and business analytics. The key to success was a combination of careful design, robust implementation, comprehensive monitoring, and continuous improvement based on operational feedback.

### Q17: What are the best practices for securing a Kafka cluster in production?
**Answer:** Securing a Kafka cluster in production requires a comprehensive approach addressing authentication, authorization, encryption, network security, and operational practices. Here's a detailed guide to Kafka security best practices:

**1. Authentication**:

- **SASL Mechanisms**:
  - **SASL/PLAIN**: Simple username/password authentication (use only with TLS)
  - **SASL/SCRAM**: Challenge-response authentication with salted passwords
  - **SASL/GSSAPI (Kerberos)**: Enterprise-grade authentication with ticket-based access
  - **SASL/OAUTHBEARER**: OAuth 2.0 token-based authentication
  
  ```properties
  # Broker configuration for SASL/SCRAM
  listeners=SASL_SSL://kafka:9093
  security.inter.broker.protocol=SASL_SSL
  sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
  sasl.enabled.mechanisms=SCRAM-SHA-512
  
  # Client configuration
  security.protocol=SASL_SSL
  sasl.mechanism=SCRAM-SHA-512
  sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="user" \
    password="password";
  ```

- **SSL/TLS Certificates**:
  - Use certificate-based authentication
  - Implement proper certificate management with rotation
  - Consider using a certificate authority (CA) for certificate signing
  
  ```properties
  # Broker configuration for SSL
  listeners=SSL://kafka:9093
  ssl.keystore.location=/path/to/kafka.server.keystore.jks
  ssl.keystore.password=keystore-password
  ssl.key.password=key-password
  ssl.truststore.location=/path/to/kafka.server.truststore.jks
  ssl.truststore.password=truststore-password
  ssl.client.auth=required
  
  # Client configuration
  security.protocol=SSL
  ssl.truststore.location=/path/to/client.truststore.jks
  ssl.truststore.password=truststore-password
  ssl.keystore.location=/path/to/client.keystore.jks
  ssl.keystore.password=keystore-password
  ssl.key.password=key-password
  ```

- **Mutual TLS (mTLS)**:
  - Require clients to authenticate with certificates
  - Validate client certificates against trusted CAs
  - Implement certificate revocation checks

**2. Authorization**:

- **ACL-based Authorization**:
  - Define fine-grained access control lists
  - Follow the principle of least privilege
  - Regularly audit and review ACLs
  
  ```properties
  # Enable ACLs on brokers
  authorizer.class.name=kafka.security.authorizer.AclAuthorizer
  super.users=User:admin;User:kafka
  ```
  
  ```bash
  # Create ACLs for a producer
  kafka-acls.sh --bootstrap-server kafka:9093 \
    --command-config admin.properties \
    --add \
    --allow-principal User:app1 \
    --producer \
    --topic orders
  
  # Create ACLs for a consumer group
  kafka-acls.sh --bootstrap-server kafka:9093 \
    --command-config admin.properties \
    --add \
    --allow-principal User:app2 \
    --consumer \
    --topic orders \
    --group order-processors
  ```

- **Role-Based Access Control (RBAC)**:
  - Use Confluent's RBAC for enterprise deployments
  - Define roles based on job functions
  - Implement role bindings to users and groups
  - Integrate with enterprise identity providers

- **Resource Patterns**:
  - Use prefixed ACLs for topic patterns
  - Implement naming conventions for topics
  - Group related resources under common prefixes

**3. Encryption**:

- **Transport Layer Encryption**:
  - Enable SSL/TLS for all communication
  - Use strong cipher suites
  - Disable vulnerable protocols (SSLv3, TLS 1.0)
  - Implement perfect forward secrecy
  
  ```properties
  # Strong SSL configuration
  ssl.enabled.protocols=TLSv1.2,TLSv1.3
  ssl.cipher.suites=TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256,TLS_AES_128_GCM_SHA256
  ssl.endpoint.identification.algorithm=HTTPS
  ```

- **Data-at-Rest Encryption**:
  - Encrypt broker disks using filesystem or volume encryption
  - Use technologies like LUKS, dm-crypt, or cloud provider encryption
  - Implement secure key management for disk encryption keys

- **End-to-End Encryption**:
  - Implement application-level encryption for sensitive data
  - Use envelope encryption with rotating data keys
  - Consider using Confluent's Secret Protection for configuration encryption

**4. Network Security**:

- **Network Segmentation**:
  - Place Kafka in a private subnet
  - Use network ACLs and security groups
  - Implement jump hosts for administrative access
  
  ```
  # AWS Security Group example
  resource "aws_security_group" "kafka" {
    name        = "kafka-broker-sg"
    description = "Security group for Kafka brokers"
    vpc_id      = aws_vpc.main.id
    
    # Inter-broker communication
    ingress {
      from_port   = 9092
      to_port     = 9092
      protocol    = "tcp"
      self        = true
    }
    
    # Client communication (from application subnet only)
    ingress {
      from_port   = 9093
      to_port     = 9093
      protocol    = "tcp"
      security_groups = [aws_security_group.application.id]
    }
  }
  ```

- **Firewalls and Load Balancers**:
  - Configure firewalls to restrict access to Kafka ports
  - Use internal load balancers for client connections
  - Implement connection throttling to prevent DoS attacks

- **VPN or Private Links**:
  - Use VPN for secure remote access
  - Implement AWS PrivateLink or Azure Private Link for cloud deployments
  - Consider service mesh for advanced traffic management

**5. Broker Hardening**:

- **Secure Configuration**:
  - Remove default or sample configurations
  - Disable unnecessary features
  - Use secure defaults for all settings
  
  ```properties
  # Secure broker defaults
  delete.topic.enable=false
  auto.create.topics.enable=false
  log.retention.hours=168
  log.cleanup.policy=delete
  ```

- **OS Hardening**:
  - Use minimal, security-focused OS distributions
  - Apply security patches regularly
  - Implement host-based firewalls
  - Use SELinux or AppArmor profiles

- **JVM Security**:
  - Apply Java security patches promptly
  - Configure appropriate JVM security settings
  - Implement JVM memory limits
  
  ```
  # Secure JVM options
  KAFKA_OPTS="-Djava.security.egd=file:/dev/urandom -Dcom.sun.management.jmxremote.authenticate=true"
  ```

- **File Permissions**:
  - Restrict access to Kafka data directories
  - Use appropriate file ownership and permissions
  - Implement filesystem ACLs if needed

**6. Monitoring and Auditing**:

- **Security Logging**:
  - Enable detailed security logging
  - Forward security logs to a SIEM system
  - Set up alerts for suspicious activities
  
  ```properties
  # Enable audit logging
  authorizer.logger.name=kafka.authorizer.logger
  log4j.logger.kafka.authorizer.logger=INFO, authorizerAppender
  ```

- **Authentication Monitoring**:
  - Track failed login attempts
  - Monitor credential usage patterns
  - Alert on unusual authentication behavior

- **Authorization Auditing**:
  - Log all authorization decisions
  - Regularly review access patterns
  - Implement automated ACL reviews

- **Compliance Monitoring**:
  - Track compliance with security policies
  - Generate reports for regulatory requirements
  - Implement automated compliance checks

**7. Operational Security**:

- **Secure Administrative Access**:
  - Use dedicated admin clients with strong authentication
  - Implement just-in-time access for administrative tasks
  - Audit all administrative actions
  
  ```properties
  # Admin client configuration
  security.protocol=SASL_SSL
  sasl.mechanism=SCRAM-SHA-512
  sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="admin" \
    password="admin-password";
  ```

- **Credential Management**:
  - Implement secure credential storage
  - Rotate credentials regularly
  - Use a secrets management solution (HashiCorp Vault, AWS Secrets Manager)
  
  ```java
  // Example of loading credentials from Vault
  public class VaultCredentialProvider implements CredentialProvider {
      private final VaultClient vaultClient;
      
      @Override
      public String getPassword(String username) {
          try {
              String path = "secret/kafka/users/" + username;
              VaultResponse response = vaultClient.logical().read(path);
              return (String) response.getData().get("password");
          } catch (VaultException e) {
              throw new AuthenticationException("Failed to retrieve credentials", e);
          }
      }
  }
  ```

- **Certificate Management**:
  - Implement automated certificate rotation
  - Monitor certificate expiration
  - Use short-lived certificates where possible

- **Secure Deployment**:
  - Implement CI/CD security scanning
  - Validate configurations before deployment
  - Use immutable infrastructure patterns

**8. Client Security**:

- **Secure Client Configuration**:
  - Ensure all clients use authentication and encryption
  - Implement credential rotation for clients
  - Validate client configurations
  
  ```java
  // Secure producer configuration
  Properties props = new Properties();
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9093");
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  
  // Security settings
  props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
  props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
  props.put(SaslConfigs.SASL_JAAS_CONFIG, 
          "org.apache.kafka.common.security.scram.ScramLoginModule required " +
          "username=\"app\" " +
          "password=\"" + getSecurePassword() + "\";");
  
  // SSL settings
  props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/path/to/truststore.jks");
  props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, getTruststorePassword());
  props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
  
  KafkaProducer<String, String> producer = new KafkaProducer<>(props);
  ```

- **Client Library Security**:
  - Keep client libraries updated
  - Use secure versions of client libraries
  - Implement client-side security controls

- **Application Security**:
  - Validate and sanitize data before producing
  - Implement application-level access controls
  - Secure sensitive data in messages

**9. Schema Registry Security**:

- **Authentication and Authorization**:
  - Secure Schema Registry with authentication
  - Implement fine-grained schema access controls
  - Restrict schema registration and evolution
  
  ```properties
  # Schema Registry security configuration
  authentication.method=BASIC
  authentication.roles=admin,developer,readonly
  authentication.realm=kafka-schema-registry-properties-file-realm
  
  # Schema Registry ACLs
  kafkastore.security.protocol=SASL_SSL
  kafkastore.sasl.mechanism=SCRAM-SHA-512
  kafkastore.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="schema-registry" \
    password="schema-registry-password";
  ```

- **Schema Validation**:
  - Implement schema validation rules
  - Enforce compatibility policies
  - Audit schema changes

**10. ZooKeeper Security** (if used):

- **Authentication**:
  - Enable SASL authentication for ZooKeeper
  - Use Kerberos or Digest authentication
  - Secure ZooKeeper-to-broker communication
  
  ```properties
  # ZooKeeper security configuration
  authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider
  requireClientAuthScheme=sasl
  jaasLoginRenew=3600000
  ```

- **Access Controls**:
  - Implement ZooKeeper ACLs
  - Restrict access to sensitive znodes
  - Use secure ZooKeeper client configuration

- **Network Security**:
  - Isolate ZooKeeper on a private network
  - Restrict access to ZooKeeper ports
  - Use TLS for ZooKeeper communication

**11. KRaft Mode Security** (Kafka 3.0+):

- **Authentication**:
  - Configure secure controller quorum communication
  - Implement mutual TLS for controller nodes
  - Secure controller-to-broker communication
  
  ```properties
  # KRaft security configuration
  controller.quorum.voters=1@controller1:9093,2@controller2:9093,3@controller3:9093
  controller.listener.names=CONTROLLER
  listeners=CONTROLLER://controller1:9093,EXTERNAL://controller1:9094
  listener.security.protocol.map=CONTROLLER:SASL_SSL,EXTERNAL:SASL_SSL
  ```

- **Authorization**:
  - Implement appropriate ACLs for controller operations
  - Restrict metadata access
  - Secure controller election process

**12. Cloud-Specific Security**:

- **AWS MSK**:
  - Use IAM authentication
  - Implement VPC endpoints
  - Enable encryption settings
  - Configure security groups properly
  
  ```terraform
  resource "aws_msk_cluster" "kafka" {
    cluster_name           = "secure-kafka-cluster"
    kafka_version          = "2.8.1"
    number_of_broker_nodes = 3
    
    broker_node_group_info {
      instance_type   = "kafka.m5.large"
      client_subnets  = aws_subnet.private[*].id
      security_groups = [aws_security_group.kafka.id]
      storage_info {
        ebs_storage_info {
          volume_size = 100
        }
      }
    }
    
    encryption_info {
      encryption_in_transit {
        client_broker = "TLS"
        in_cluster    = true
      }
      encryption_at_rest_kms_key_arn = aws_kms_key.kafka.arn
    }
    
    client_authentication {
      sasl {
        scram = true
      }
      tls {
        certificate_authority_arns = [aws_acmpca_certificate_authority.kafka.arn]
      }
    }
  }
  ```

- **Confluent Cloud**:
  - Use API keys with appropriate permissions
  - Implement network egress filtering
  - Enable private link where available
  - Configure RBAC properly

- **Azure HDInsight**:
  - Use Azure AD integration
  - Implement VNet injection
  - Enable disk encryption
  - Configure network security groups

**13. Security Testing and Validation**:

- **Penetration Testing**:
  - Conduct regular security assessments
  - Test authentication and authorization controls
  - Validate encryption implementation

- **Configuration Validation**:
  - Use automated tools to validate security configurations
  - Implement security as code
  - Validate against security benchmarks

- **Vulnerability Scanning**:
  - Regularly scan Kafka infrastructure
  - Monitor for new vulnerabilities
  - Implement automated patching

**14. Incident Response**:

- **Security Monitoring**:
  - Implement real-time security monitoring
  - Set up alerts for security events
  - Correlate security logs across systems

- **Incident Playbooks**:
  - Develop specific playbooks for Kafka security incidents
  - Practice response procedures
  - Document containment and recovery steps

- **Forensic Readiness**:
  - Configure appropriate logging for forensic analysis
  - Implement log preservation procedures
  - Establish chain of custody processes

**Implementation Example - Comprehensive Security Setup**:

```java
// Secure Kafka client factory
@Configuration
public class SecureKafkaConfig {
    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;
    
    @Value("${kafka.security.protocol}")
    private String securityProtocol;
    
    @Value("${kafka.sasl.mechanism}")
    private String saslMechanism;
    
    @Autowired
    private SecretManager secretManager;
    
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        
        // Security configuration
        configureSecurityProperties(props);
        
        // Producer-specific security settings
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        
        return new DefaultKafkaProducerFactory<>(props);
    }
    
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "secure-consumer-group");
        
        // Security configuration
        configureSecurityProperties(props);
        
        // Consumer-specific security settings
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.company.model");
        
        return new DefaultKafkaConsumerFactory<>(props);
    }
    
    @Bean
    public AdminClient kafkaAdminClient() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // Security configuration
        configureSecurityProperties(props);
        
        return AdminClient.create(props);
    }
    
    private void configureSecurityProperties(Map<String, Object> props) {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        
        if ("SASL_SSL".equals(securityProtocol) || "SASL_PLAINTEXT".equals(securityProtocol)) {
            props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            
            // Get credentials from secure storage
            String username = secretManager.getSecret("kafka/username");
            String password = secretManager.getSecret("kafka/password");
            
            String jaasConfig = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\"%s\" " +
                "password=\"%s\";",
                username, password
            );
            props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        }
        
        if ("SSL".equals(securityProtocol) || "SASL_SSL".equals(securityProtocol)) {
            // SSL configuration
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, 
                     secretManager.getFilePath("kafka/truststore"));
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, 
                     secretManager.getSecret("kafka/truststore_password"));
            
            // For mutual TLS
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, 
                     secretManager.getFilePath("kafka/keystore"));
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, 
                     secretManager.getSecret("kafka/keystore_password"));
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, 
                     secretManager.getSecret("kafka/key_password"));
            
            // Additional SSL settings
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
            props.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2,TLSv1.3");
        }
    }
    
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Configure error handling
        factory.setErrorHandler(new SeekToCurrentErrorHandler(
            new DeadLetterPublishingRecoverer(kafkaTemplate()), 
            new FixedBackOff(1000L, 3)));
            
        return factory;
    }
    
    // Credential rotation scheduler
    @Scheduled(cron = "0 0 0 * * 0") // Weekly rotation
    public void rotateKafkaCredentials() {
        try {
            log.info("Starting scheduled Kafka credential rotation");
            
            // Generate new credentials
            String newPassword = generateSecurePassword();
            
            // Update credentials in secure storage
            secretManager.updateSecret("kafka/password", newPassword);
            
            // Update credentials in Kafka
            AdminClient adminClient = kafkaAdminClient();
            AlterUserScramCredentialsResult result = adminClient.alterUserScramCredentials(
                Collections.singletonList(
                    new UserScramCredentialUpsertion(
                        secretManager.getSecret("kafka/username"),
                        ScramMechanism.SCRAM_SHA_512,
                        newPassword
                    )
                )
            );
            result.all().get(30, TimeUnit.SECONDS);
            
            log.info("Kafka credential rotation completed successfully");
            
            // Trigger client reconnection
            applicationEventPublisher.publishEvent(new CredentialRotationEvent());
        } catch (Exception e) {
            log.error("Failed to rotate Kafka credentials", e);
            alertService.sendAlert("Kafka Credential Rotation Failed", e.getMessage());
        }
    }
    
    private String generateSecurePassword() {
        // Generate a secure random password
        return new SecureRandom()
            .ints(24, 33, 122)
            .mapToObj(i -> String.valueOf((char) i))
            .collect(Collectors.joining());
    }
}
```

By implementing these security best practices, you can create a robust security posture for your Kafka deployment that protects against unauthorized access, ensures data confidentiality and integrity, and meets regulatory compliance requirements.

### Q18: How would you approach migrating from a traditional messaging system to Kafka?
**Answer:** Migrating from a traditional messaging system (like RabbitMQ, ActiveMQ, IBM MQ, or JMS) to Apache Kafka requires careful planning and execution to minimize disruption while maximizing the benefits of Kafka's architecture. Here's a comprehensive approach to such a migration:

**1. Assessment and Planning Phase**:

- **Current System Analysis**:
  - Document existing message flows, patterns, and volumes
  - Identify message schemas and data formats
  - Map out producer and consumer applications
  - Analyze current performance metrics and SLAs
  - Document existing failure handling and recovery mechanisms
  
  ```
  # Example message flow documentation
  Message Flow: Order Processing
  - Source: Order Service (produces 100 msgs/sec peak)
  - Destination: Inventory Service, Shipping Service, Billing Service
  - Message Format: XML with XSD schema
  - Current Queue: order_processing_queue
  - Delivery Requirements: At-least-once
  - Retention: 3 days
  - Current Issues: Occasional backpressure during peak hours
  ```

- **Kafka Architecture Design**:
  - Design topic structure and partitioning strategy
  - Determine appropriate replication factor
  - Plan consumer group strategy
  - Design schema evolution approach
  - Determine retention policies
  
  ```
  # Example Kafka design decisions
  Topic: orders
  - Partitions: 12 (based on throughput analysis)
  - Replication Factor: 3
  - Partitioning Key: orderId
  - Retention: 7 days
  - Format: Avro with Schema Registry
  - Consumer Groups:
    * inventory-processors (3 instances)
    * shipping-processors (2 instances)
    * billing-processors (2 instances)
  ```

- **Gap Analysis**:
  - Identify differences in messaging semantics
  - Analyze transaction support requirements
  - Evaluate message ordering guarantees
  - Assess message filtering capabilities
  - Determine dead letter queue requirements
  
  ```
  # Example gap analysis
  1. Message Selectors: Current system uses JMS selectors, Kafka requires filtering in consumer code
  2. Transactions: Current system uses XA transactions, Kafka requires application-level coordination
  3. Message TTL: Current system expires messages, Kafka uses topic-level retention
  4. Priority Queues: Current system has 3 priority levels, Kafka needs separate topics or custom logic
  5. Request-Reply: Current system has built-in support, Kafka requires correlation ID pattern
  ```

- **Migration Strategy Selection**:
  - Parallel run with dual writing
  - Incremental migration by message flow
  - Big bang migration
  - Strangler pattern approach
  
  ```
  # Example migration strategy
  Approach: Incremental migration by message flow
  1. Start with non-critical, lower-volume flows
  2. Implement dual-write for each flow during transition
  3. Migrate consumers before producers
  4. Validate each flow before moving to the next
  5. Maintain rollback capability for each migration step
  ```

**2. Infrastructure Setup**:

- **Kafka Cluster Deployment**:
  - Set up development, testing, and production environments
  - Configure appropriate hardware or cloud resources
  - Implement monitoring and alerting
  - Establish operational procedures
  
  ```yaml
  # Example Kubernetes deployment for Kafka
  apiVersion: kafka.strimzi.io/v1beta2
  kind: Kafka
  metadata:
    name: migration-kafka-cluster
  spec:
    kafka:
      version: 3.3.1
      replicas: 3
      listeners:
        - name: plain
          port: 9092
          type: internal
          tls: false
        - name: tls
          port: 9093
          type: internal
          tls: true
      config:
        offsets.topic.replication.factor: 3
        transaction.state.log.replication.factor: 3
        transaction.state.log.min.isr: 2
        default.replication.factor: 3
        min.insync.replicas: 2
        inter.broker.protocol.version: "3.3"
      storage:
        type: jbod
        volumes:
        - id: 0
          type: persistent-claim
          size: 100Gi
          deleteClaim: false
    zookeeper:
      replicas: 3
      storage:
        type: persistent-claim
        size: 20Gi
        deleteClaim: false
    entityOperator:
      topicOperator: {}
      userOperator: {}
  ```

- **Schema Registry Setup** (if using Avro/Protobuf/JSON Schema):
  - Deploy Schema Registry
  - Configure schema compatibility settings
  - Establish schema governance process
  
  ```yaml
  # Example Schema Registry deployment
  apiVersion: apps/v1
  kind: Deployment
  metadata:
    name: schema-registry
  spec:
    replicas: 2
    selector:
      matchLabels:
        app: schema-registry
    template:
      metadata:
        labels:
          app: schema-registry
      spec:
        containers:
        - name: schema-registry
          image: confluentinc/cp-schema-registry:7.3.0
          ports:
          - containerPort: 8081
          env:
          - name: SCHEMA_REGISTRY_HOST_NAME
            value: schema-registry
          - name: SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS
            value: kafka:9092
          - name: SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL
            value: SASL_SSL
          - name: SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL
            value: BACKWARD
  ```

- **Kafka Connect Setup** (if needed for CDC or integration):
  - Deploy Kafka Connect cluster
  - Configure required connectors
  - Test connector functionality
  
  ```yaml
  # Example Kafka Connect deployment
  apiVersion: kafka.strimzi.io/v1beta2
  kind: KafkaConnect
  metadata:
    name: migration-connect-cluster
  spec:
    version: 3.3.1
    replicas: 3
    bootstrapServers: kafka:9092
    config:
      group.id: connect-cluster
      offset.storage.topic: connect-offsets
      config.storage.topic: connect-configs
      status.storage.topic: connect-status
      key.converter: org.apache.kafka.connect.storage.StringConverter
      value.converter: io.confluent.connect.avro.AvroConverter
      value.converter.schema.registry.url: http://schema-registry:8081
    build:
      output:
        type: docker
        image: migration-connect:latest
      plugins:
        - name: debezium-mongodb-connector
          artifacts:
            - type: jar
              url: https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/1.9.6.Final/debezium-connector-mongodb-1.9.6.Final-plugin.tar.gz
        - name: jms-connector
          artifacts:
            - type: jar
              url: https://repo1.maven.org/maven2/org/apache/camel/kafkaconnector/camel-sjms2-kafka-connector/3.20.1/camel-sjms2-kafka-connector-3.20.1-package.tar.gz
  ```

**3. Development and Testing**:

- **Client Library Migration**:
  - Update producer applications to use Kafka client libraries
  - Modify consumer applications to use Kafka consumer API
  - Implement appropriate error handling and retry logic
  
  ```java
  // Example: Migrating from JMS to Kafka Producer
  
  // Original JMS code
  public void sendOrderMessage(Order order) {
      try {
          Connection connection = connectionFactory.createConnection();
          Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
          MessageProducer producer = session.createProducer(orderQueue);
          
          TextMessage message = session.createTextMessage(orderToXml(order));
          message.setStringProperty("orderId", order.getId());
          message.setJMSPriority(order.isRush() ? 9 : 4);
          message.setJMSExpiration(System.currentTimeMillis() + 86400000);
          
          producer.send(message);
          session.close();
          connection.close();
      } catch (JMSException e) {
          throw new MessageSendException("Failed to send order message", e);
      }
  }
  
  // Migrated Kafka code
  public void sendOrderMessage(Order order) {
      try {
          String orderJson = objectMapper.writeValueAsString(order);
          ProducerRecord<String, String> record = new ProducerRecord<>(
              "orders",
              order.getId(),  // Partitioning key
              orderJson
          );
          
          // Add headers for metadata
          record.headers().add("isRush", order.isRush() ? new byte[]{1} : new byte[]{0});
          
          kafkaProducer.send(record, (metadata, exception) -> {
              if (exception != null) {
                  log.error("Failed to send order message", exception);
              } else {
                  log.debug("Order message sent to partition {} at offset {}", 
                           metadata.partition(), metadata.offset());
              }
          });
      } catch (JsonProcessingException e) {
          throw new MessageSendException("Failed to serialize order", e);
      }
  }
  ```
  
  ```java
  // Example: Migrating from JMS to Kafka Consumer
  
  // Original JMS code
  public void startOrderConsumer() {
      try {
          Connection connection = connectionFactory.createConnection();
          Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
          MessageConsumer consumer = session.createConsumer(
              orderQueue, 
              "orderType = 'RETAIL'"
          );
          
          consumer.setMessageListener(message -> {
              try {
                  TextMessage textMessage = (TextMessage) message;
                  Order order = xmlToOrder(textMessage.getText());
                  processOrder(order);
                  message.acknowledge();
              } catch (Exception e) {
                  log.error("Error processing message", e);
                  // Message will be redelivered since not acknowledged
              }
          });
          
          connection.start();
      } catch (JMSException e) {
          throw new MessageConsumptionException("Failed to start order consumer", e);
      }
  }
  
  // Migrated Kafka code
  public void startOrderConsumer() {
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-processors");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
      consumer.subscribe(Collections.singletonList("orders"));
      
      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.submit(() -> {
          try {
              while (true) {
                  ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                  
                  for (ConsumerRecord<String, String> record : records) {
                      try {
                          Order order = objectMapper.readValue(record.value(), Order.class);
                          
                          // Filter for retail orders (replacing JMS selector)
                          if ("RETAIL".equals(order.getOrderType())) {
                              processOrder(order);
                          }
                          
                      } catch (Exception e) {
                          log.error("Error processing record", e);
                          // Error handling strategy (DLQ, retry, etc.)
                      }
                  }
                  
                  consumer.commitSync();
              }
          } catch (WakeupException e) {
              // Shutdown signal
          } finally {
              consumer.close();
          }
      });
      
      // Shutdown hook
      Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
  }
  ```

- **Message Format Conversion**:
  - Implement serialization/deserialization logic
  - Set up schema registry if using Avro/Protobuf
  - Create schema evolution strategy
  
  ```java
  // Example: Converting from XML to Avro
  
  // Schema definition
  String avroSchema = "{"
      + "\"type\": \"record\","
      + "\"name\": \"Order\","
      + "\"fields\": ["
      + "  {\"name\": \"id\", \"type\": \"string\"},"
      + "  {\"name\": \"customerId\", \"type\": \"string\"},"
      + "  {\"name\": \"orderType\", \"type\": \"string\"},"
      + "  {\"name\": \"amount\", \"type\": \"double\"},"
      + "  {\"name\": \"items\", \"type\": {\"type\": \"array\", \"items\": {"
      + "    \"type\": \"record\","
      + "    \"name\": \"OrderItem\","
      + "    \"fields\": ["
      + "      {\"name\": \"productId\", \"type\": \"string\"},"
      + "      {\"name\": \"quantity\", \"type\": \"int\"},"
      + "      {\"name\": \"price\", \"type\": \"double\"}"
      + "    ]"
      + "  }}},"
      + "  {\"name\": \"isRush\", \"type\": \"boolean\", \"default\": false}"
      + "]"
      + "}";
  
  // Converter implementation
  public class OrderConverter {
      private final Schema schema;
      private final SchemaRegistryClient schemaRegistry;
      private final String topic;
      private Integer schemaId;
      
      public OrderConverter(String schemaStr, SchemaRegistryClient schemaRegistry, String topic) {
          this.schema = new Schema.Parser().parse(schemaStr);
          this.schemaRegistry = schemaRegistry;
          this.topic = topic;
      }
      
      public byte[] convertToAvro(Order order) throws Exception {
          // Register schema if needed
          if (schemaId == null) {
              schemaId = schemaRegistry.register(topic + "-value", schema);
          }
          
          // Create Avro record
          GenericRecord avroRecord = new GenericData.Record(schema);
          avroRecord.put("id", order.getId());
          avroRecord.put("customerId", order.getCustomerId());
          avroRecord.put("orderType", order.getOrderType());
          avroRecord.put("amount", order.getAmount());
          avroRecord.put("isRush", order.isRush());
          
          // Convert order items
          List<GenericRecord> avroItems = new ArrayList<>();
          Schema itemSchema = schema.getField("items").schema().getElementType();
          
          for (OrderItem item : order.getItems()) {
              GenericRecord avroItem = new GenericData.Record(itemSchema);
              avroItem.put("productId", item.getProductId());
              avroItem.put("quantity", item.getQuantity());
              avroItem.put("price", item.getPrice());
              avroItems.add(avroItem);
          }
          avroRecord.put("items", avroItems);
          
          // Serialize with schema ID
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          out.write(0); // Magic byte
          out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
          
          // Write the avro data
          BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
          DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
          writer.write(avroRecord, encoder);
          encoder.flush();
          
          return out.toByteArray();
      }
      
      public Order convertFromAvro(byte[] bytes) throws Exception {
          // Extract schema ID
          ByteBuffer buffer = ByteBuffer.wrap(bytes);
          byte magicByte = buffer.get();
          int id = buffer.getInt();
          
          // Get schema from registry if different
          Schema readerSchema;
          if (schemaId == null || schemaId != id) {
              schemaId = id;
              readerSchema = new Schema.Parser().parse(schemaRegistry.getById(id).getSchema());
          } else {
              readerSchema = schema;
          }
          
          // Deserialize avro data
          DatumReader<GenericRecord> reader = new GenericDatumReader<>(readerSchema);
          BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(
              bytes, 5, bytes.length - 5, null);
          GenericRecord record = reader.read(null, decoder);
          
          // Convert to domain object
          Order order = new Order();
          order.setId(record.get("id").toString());
          order.setCustomerId(record.get("customerId").toString());
          order.setOrderType(record.get("orderType").toString());
          order.setAmount((Double) record.get("amount"));
          order.setRush((Boolean) record.get("isRush"));
          
          // Convert items
          List<OrderItem> items = new ArrayList<>();
          for (GenericRecord itemRecord : (List<GenericRecord>) record.get("items")) {
              OrderItem item = new OrderItem();
              item.setProductId(itemRecord.get("productId").toString());
              item.setQuantity((Integer) itemRecord.get("quantity"));
              item.setPrice((Double) itemRecord.get("price"));
              items.add(item);
          }
          order.setItems(items);
          
          return order;
      }
  }
  ```

- **Messaging Pattern Adaptation**:
  - Implement request-reply pattern if needed
  - Adapt publish-subscribe patterns to Kafka topics
  - Implement message filtering strategies
  
  ```java
  // Example: Implementing request-reply pattern in Kafka
  
  // Request sender
  public class RequestReplyClient {
      private final KafkaProducer<String, String> producer;
      private final KafkaConsumer<String, String> consumer;
      private final String requestTopic;
      private final String replyTopic;
      private final String consumerGroup;
      private final Map<String, CompletableFuture<String>> pendingRequests = new ConcurrentHashMap<>();
      private final ExecutorService executor = Executors.newSingleThreadExecutor();
      
      public RequestReplyClient(String bootstrapServers, String requestTopic, String replyTopic) {
          this.requestTopic = requestTopic;
          this.replyTopic = replyTopic;
          this.consumerGroup = "reply-consumer-" + UUID.randomUUID().toString();
          
          // Configure producer
          Properties producerProps = new Properties();
          producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
          producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
          producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
          this.producer = new KafkaProducer<>(producerProps);
          
          // Configure consumer
          Properties consumerProps = new Properties();
          consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
          consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
          consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
          consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
          this.consumer = new KafkaConsumer<>(consumerProps);
          
          // Start reply consumer
          consumer.subscribe(Collections.singletonList(replyTopic));
          executor.submit(this::consumeReplies);
      }
      
      public CompletableFuture<String> sendRequest(String key, String request, Duration timeout) {
          String correlationId = UUID.randomUUID().toString();
          CompletableFuture<String> future = new CompletableFuture<>();
          pendingRequests.put(correlationId, future);
          
          // Set up timeout
          ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
          scheduler.schedule(() -> {
              CompletableFuture<String> pendingFuture = pendingRequests.remove(correlationId);
              if (pendingFuture != null) {
                  pendingFuture.completeExceptionally(new TimeoutException("Request timed out"));
              }
              scheduler.shutdown();
          }, timeout.toMillis(), TimeUnit.MILLISECONDS);
          
          // Send request
          ProducerRecord<String, String> record = new ProducerRecord<>(requestTopic, key, request);
          record.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));
          record.headers().add("replyTo", replyTopic.getBytes(StandardCharsets.UTF_8));
          
          producer.send(record, (metadata, exception) -> {
              if (exception != null) {
                  CompletableFuture<String> pendingFuture = pendingRequests.remove(correlationId);
                  if (pendingFuture != null) {
                      pendingFuture.completeExceptionally(exception);
                  }
              }
          });
          
          return future;
      }
      
      private void consumeReplies() {
          try {
              while (true) {
                  ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                  
                  for (ConsumerRecord<String, String> record : records) {
                      String correlationId = null;
                      for (Header header : record.headers()) {
                          if ("correlationId".equals(header.key())) {
                              correlationId = new String(header.value(), StandardCharsets.UTF_8);
                              break;
                          }
                      }
                      
                      if (correlationId != null) {
                          CompletableFuture<String> future = pendingRequests.remove(correlationId);
                          if (future != null) {
                              future.complete(record.value());
                          }
                      }
                  }
              }
          } catch (WakeupException e) {
              // Shutdown signal
          } finally {
              consumer.close();
          }
      }
      
      public void close() {
          consumer.wakeup();
          executor.shutdown();
          producer.close();
          
          // Complete all pending requests with exception
          for (CompletableFuture<String> future : pendingRequests.values()) {
              future.completeExceptionally(new IllegalStateException("Client closed"));
          }
          pendingRequests.clear();
      }
  }
  
  // Reply sender (server side)
  public class RequestReplyServer {
      private final KafkaConsumer<String, String> consumer;
      private final KafkaProducer<String, String> producer;
      private final String requestTopic;
      private final Function<String, String> requestHandler;
      private final ExecutorService executor = Executors.newSingleThreadExecutor();
      
      public RequestReplyServer(String bootstrapServers, String requestTopic, 
                               String consumerGroup, Function<String, String> requestHandler) {
          this.requestTopic = requestTopic;
          this.requestHandler = requestHandler;
          
          // Configure consumer
          Properties consumerProps = new Properties();
          consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
          consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
          consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
          this.consumer = new KafkaConsumer<>(consumerProps);
          
          // Configure producer
          Properties producerProps = new Properties();
          producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
          producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
          producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
          this.producer = new KafkaProducer<>(producerProps);
          
          // Start request consumer
          consumer.subscribe(Collections.singletonList(requestTopic));
          executor.submit(this::consumeRequests);
      }
      
      private void consumeRequests() {
          try {
              while (true) {
                  ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                  
                  for (ConsumerRecord<String, String> record : records) {
                      String correlationId = null;
                      String replyTo = null;
                      
                      // Extract headers
                      for (Header header : record.headers()) {
                          if ("correlationId".equals(header.key())) {
                              correlationId = new String(header.value(), StandardCharsets.UTF_8);
                          } else if ("replyTo".equals(header.key())) {
                              replyTo = new String(header.value(), StandardCharsets.UTF_8);
                          }
                      }
                      
                      if (correlationId != null && replyTo != null) {
                          try {
                              // Process request
                              String response = requestHandler.apply(record.value());
                              
                              // Send reply
                              ProducerRecord<String, String> replyRecord = 
                                  new ProducerRecord<>(replyTo, record.key(), response);
                              replyRecord.headers().add(
                                  "correlationId", correlationId.getBytes(StandardCharsets.UTF_8));
                              
                              producer.send(replyRecord);
                          } catch (Exception e) {
                              log.error("Error processing request", e);
                          }
                      }
                  }
              }
          } catch (WakeupException e) {
              // Shutdown signal
          } finally {
              consumer.close();
          }
      }
      
      public void close() {
          consumer.wakeup();
          executor.shutdown();
          producer.close();
      }
  }
  ```

- **Transaction Handling**:
  - Implement Kafka transactions if needed
  - Create compensating transaction patterns
  - Adapt XA transactions to Kafka patterns
  
  ```java
  // Example: Implementing transactional processing
  
  public class TransactionalOrderProcessor {
      private final KafkaProducer<String, String> producer;
      private final KafkaConsumer<String, String> consumer;
      private final OrderRepository orderRepository;
      
      public TransactionalOrderProcessor(String bootstrapServers, 
                                       String consumerGroup,
                                       OrderRepository orderRepository) {
          this.orderRepository = orderRepository;
          
          // Configure transactional producer
          Properties producerProps = new Properties();
          producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
          producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
          producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
          producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-processor-" + UUID.randomUUID());
          producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
          producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
          this.producer = new KafkaProducer<>(producerProps);
          producer.initTransactions();
          
          // Configure consumer
          Properties consumerProps = new Properties();
          consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
          consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
          consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
          consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
          this.consumer = new KafkaConsumer<>(consumerProps);
      }
      
      public void processOrdersTransactionally() {
          consumer.subscribe(Collections.singletonList("orders"));
          
          try {
              while (true) {
                  ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                  
                  if (!records.isEmpty()) {
                      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                      
                      // Collect offsets
                      for (TopicPartition partition : records.partitions()) {
                          List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                          long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                          offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                      }
                      
                      try {
                          // Begin transaction
                          producer.beginTransaction();
                          
                          // Process each record
                          for (ConsumerRecord<String, String> record : records) {
                              Order order = objectMapper.readValue(record.value(), Order.class);
                              
                              // Update database
                              orderRepository.save(order);
                              
                              // Send derived events
                              if (order.getAmount() > 1000) {
                                  producer.send(new ProducerRecord<>("large-orders", 
                                                                   order.getId(), 
                                                                   record.value()));
                              }
                              
                              producer.send(new ProducerRecord<>("order-notifications", 
                                                               order.getCustomerId(), 
                                                               createNotification(order)));
                          }
                          
                          // Commit offsets in transaction
                          producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                          
                          // Commit transaction
                          producer.commitTransaction();
                      } catch (Exception e) {
                          // Abort transaction on error
                          producer.abortTransaction();
                          log.error("Transaction aborted", e);
                      }
                  }
              }
          } finally {
              consumer.close();
              producer.close();
          }
      }
      
      private String createNotification(Order order) throws JsonProcessingException {
          Notification notification = new Notification(
              "ORDER_CREATED",
              order.getId(),
              "Your order has been received",
              LocalDateTime.now()
          );
          return objectMapper.writeValueAsString(notification);
      }
  }
  ```

- **Error Handling and DLQ Implementation**:
  - Create dead letter queue topics
  - Implement retry mechanisms
  - Set up error monitoring
  
  ```java
  // Example: Dead Letter Queue implementation
  
  public class DeadLetterQueueConsumer {
      private final KafkaConsumer<String, String> consumer;
      private final KafkaProducer<String, String> producer;
      private final String sourceTopic;
      private final String dlqTopic;
      private final int maxRetries;
      
      public DeadLetterQueueConsumer(String bootstrapServers, 
                                    String consumerGroup,
                                    String sourceTopic,
                                    String dlqTopic,
                                    int maxRetries) {
          this.sourceTopic = sourceTopic;
          this.dlqTopic = dlqTopic;
          this.maxRetries = maxRetries;
          
          // Configure consumer
          Properties consumerProps = new Properties();
          consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
          consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
          consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
          this.consumer = new KafkaConsumer<>(consumerProps);
          
          // Configure producer
          Properties producerProps = new Properties();
          producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
          producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
          producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
          this.producer = new KafkaProducer<>(producerProps);
      }
      
      public void consumeWithDLQ() {
          consumer.subscribe(Collections.singletonList(sourceTopic));
          
          try {
              while (true) {
                  ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                  
                  for (ConsumerRecord<String, String> record : records) {
                      try {
                          // Process the record
                          processRecord(record);
                      } catch (Exception e) {
                          // Handle the error
                          handleProcessingError(record, e);
                      }
                  }
                  
                  consumer.commitSync();
              }
          } finally {
              consumer.close();
              producer.close();
          }
      }
      
      private void processRecord(ConsumerRecord<String, String> record) {
          // Actual processing logic
          // ...
          
          // If processing fails, throw an exception
      }
      
      private void handleProcessingError(ConsumerRecord<String, String> record, Exception e) {
          // Get retry count from headers
          int retryCount = 0;
          for (Header header : record.headers()) {
              if ("retry-count".equals(header.key())) {
                  retryCount = ByteBuffer.wrap(header.value()).getInt();
                  break;
              }
          }
          
          if (retryCount < maxRetries) {
              // Retry by sending back to source topic
              ProducerRecord<String, String> retryRecord = 
                  new ProducerRecord<>(sourceTopic, record.key(), record.value());
              
              // Copy existing headers
              for (Header header : record.headers()) {
                  if (!"retry-count".equals(header.key())) {
                      retryRecord.headers().add(header);
                  }
              }
              
              // Update retry count
              retryRecord.headers().add("retry-count", 
                                      ByteBuffer.allocate(4).putInt(retryCount + 1).array());
              
              // Add error information
              retryRecord.headers().add("last-error", e.getMessage().getBytes(StandardCharsets.UTF_8));
              retryRecord.headers().add("last-error-time", 
                                      String.valueOf(System.currentTimeMillis())
                                          .getBytes(StandardCharsets.UTF_8));
              
              // Send with exponential backoff
              long backoffMs = (long) Math.pow(2, retryCount) * 1000;
              ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
              scheduler.schedule(() -> {
                  producer.send(retryRecord);
                  scheduler.shutdown();
              }, backoffMs, TimeUnit.MILLISECONDS);
              
              log.warn("Message processing failed, scheduled retry {} in {} ms", 
                      retryCount + 1, backoffMs, e);
          } else {
              // Max retries exceeded, send to DLQ
              ProducerRecord<String, String> dlqRecord = 
                  new ProducerRecord<>(dlqTopic, record.key(), record.value());
              
              // Copy all headers
              record.headers().forEach(header -> dlqRecord.headers().add(header));
              
              // Add error information
              dlqRecord.headers().add("final-error", e.getMessage().getBytes(StandardCharsets.UTF_8));
              dlqRecord.headers().add("final-error-time", 
                                    String.valueOf(System.currentTimeMillis())
                                        .getBytes(StandardCharsets.UTF_8));
              dlqRecord.headers().add("source-topic", sourceTopic.getBytes(StandardCharsets.UTF_8));
              dlqRecord.headers().add("source-partition", 
                                    ByteBuffer.allocate(4).putInt(record.partition()).array());
              dlqRecord.headers().add("source-offset", 
                                    ByteBuffer.allocate(8).putLong(record.offset()).array());
              
              producer.send(dlqRecord);
              log.error("Message processing failed after {} retries, sent to DLQ", maxRetries, e);
          }
      }
  }
  ```

- **Performance Testing**:
  - Benchmark Kafka vs. existing system
  - Test under various load conditions
  - Validate latency and throughput requirements
  
  ```java
  // Example: Performance testing framework
  
  public class KafkaPerformanceTester {
      private final String bootstrapServers;
      private final String topic;
      private final int messageCount;
      private final int messageSize;
      private final int producerThreads;
      private final int consumerThreads;
      
      public KafkaPerformanceTester(String bootstrapServers, 
                                   String topic, 
                                   int messageCount, 
                                   int messageSize,
                                   int producerThreads,
                                   int consumerThreads) {
          this.bootstrapServers = bootstrapServers;
          this.topic = topic;
          this.messageCount = messageCount;
          this.messageSize = messageSize;
          this.producerThreads = producerThreads;
          this.consumerThreads = consumerThreads;
      }
      
      public PerformanceResult runTest() throws InterruptedException {
          // Generate test data
          byte[] messageData = new byte[messageSize];
          new Random().nextBytes(messageData);
          String messageValue = Base64.getEncoder().encodeToString(messageData);
          
          // Start consumers
          CountDownLatch consumerLatch = new CountDownLatch(messageCount);
          List<ConsumerWorker> consumers = new ArrayList<>();
          List<Thread> consumerThreadsList = new ArrayList<>();
          
          for (int i = 0; i < consumerThreads; i++) {
              ConsumerWorker consumer = new ConsumerWorker(
                  bootstrapServers, 
                  topic, 
                  "perf-consumer-group",
                  consumerLatch
              );
              consumers.add(consumer);
              Thread thread = new Thread(consumer);
              consumerThreadsList.add(thread);
              thread.start();
          }
          
          // Wait for consumers to initialize
          Thread.sleep(5000);
          
          // Start producers
          CountDownLatch producerLatch = new CountDownLatch(producerThreads);
          List<ProducerWorker> producers = new ArrayList<>();
          List<Thread> producerThreadsList = new ArrayList<>();
          
          long startTime = System.currentTimeMillis();
          
          for (int i = 0; i < producerThreads; i++) {
              ProducerWorker producer = new ProducerWorker(
                  bootstrapServers,
                  topic,
                  messageCount / producerThreads,
                  messageValue,
                  producerLatch
              );
              producers.add(producer);
              Thread thread = new Thread(producer);
              producerThreadsList.add(thread);
              thread.start();
          }
          
          // Wait for producers to finish
          producerLatch.await();
          long producerEndTime = System.currentTimeMillis();
          
          // Wait for consumers to finish
          boolean consumersFinished = consumerLatch.await(60, TimeUnit.SECONDS);
          long consumerEndTime = System.currentTimeMillis();
          
          // Stop consumers
          for (ConsumerWorker consumer : consumers) {
              consumer.shutdown();
          }
          
          for (Thread thread : consumerThreadsList) {
              thread.join(5000);
          }
          
          // Calculate results
          long producerElapsed = producerEndTime - startTime;
          long consumerElapsed = consumerEndTime - startTime;
          
          double producerThroughput = (double) messageCount / (producerElapsed / 1000.0);
          double consumerThroughput = consumersFinished ? 
              (double) messageCount / (consumerElapsed / 1000.0) : 0;
          
          // Collect latencies
          List<Long> latencies = new ArrayList<>();
          for (ConsumerWorker consumer : consumers) {
              latencies.addAll(consumer.getLatencies());
          }
          
          Collections.sort(latencies);
          long minLatency = latencies.isEmpty() ? 0 : latencies.get(0);
          long maxLatency = latencies.isEmpty() ? 0 : latencies.get(latencies.size() - 1);
          long p50Latency = latencies.isEmpty() ? 0 : latencies.get(latencies.size() / 2);
          long p95Latency = latencies.isEmpty() ? 0 : 
              latencies.get((int) (latencies.size() * 0.95));
          long p99Latency = latencies.isEmpty() ? 0 : 
              latencies.get((int) (latencies.size() * 0.99));
          
          return new PerformanceResult(
              messageCount,
              messageSize,
              producerThreads,
              consumerThreads,
              producerThroughput,
              consumerThroughput,
              minLatency,
              maxLatency,
              p50Latency,
              p95Latency,
              p99Latency
          );
      }
      
      private static class ProducerWorker implements Runnable {
          private final KafkaProducer<String, String> producer;
          private final String topic;
          private final int messageCount;
          private final String messageValue;
          private final CountDownLatch latch;
          
          public ProducerWorker(String bootstrapServers, 
                              String topic, 
                              int messageCount, 
                              String messageValue,
                              CountDownLatch latch) {
              Properties props = new Properties();
              props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
              props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
              props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
              props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
              props.put(ProducerConfig.BATCH_SIZE_CONFIG, "64000");
              props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
              
              this.producer = new KafkaProducer<>(props);
              this.topic = topic;
              this.messageCount = messageCount;
              this.messageValue = messageValue;
              this.latch = latch;
          }
          
          @Override
          public void run() {
              try {
                  for (int i = 0; i < messageCount; i++) {
                      String key = UUID.randomUUID().toString();
                      ProducerRecord<String, String> record = 
                          new ProducerRecord<>(topic, key, messageValue);
                      
                      // Add timestamp header for latency measurement
                      record.headers().add("timestamp", 
                                         ByteBuffer.allocate(8)
                                             .putLong(System.currentTimeMillis())
                                             .array());
                      
                      producer.send(record);
                  }
              } finally {
                  producer.close();
                  latch.countDown();
              }
          }
      }
      
      private static class ConsumerWorker implements Runnable {
          private final KafkaConsumer<String, String> consumer;
          private final CountDownLatch latch;
          private final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
          private volatile boolean running = true;
          
          public ConsumerWorker(String bootstrapServers, 
                              String topic, 
                              String groupId,
                              CountDownLatch latch) {
              Properties props = new Properties();
              props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
              props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
              props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
              props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
              props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
              props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
              props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
              
              this.consumer = new KafkaConsumer<>(props);
              this.consumer.subscribe(Collections.singletonList(topic));
              this.latch = latch;
          }
          
          @Override
          public void run() {
              try {
                  while (running) {
                      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                      
                      for (ConsumerRecord<String, String> record : records) {
                          // Calculate latency
                          long now = System.currentTimeMillis();
                          long timestamp = 0;
                          
                          for (Header header : record.headers()) {
                              if ("timestamp".equals(header.key())) {
                                  timestamp = ByteBuffer.wrap(header.value()).getLong();
                                  break;
                              }
                          }
                          
                          if (timestamp > 0) {
                              latencies.add(now - timestamp);
                          }
                          
                          latch.countDown();
                      }
                  }
              } catch (WakeupException e) {
                  // Ignore, this is expected during shutdown
              } finally {
                  consumer.close();
              }
          }
          
          public void shutdown() {
              running = false;
              consumer.wakeup();
          }
          
          public List<Long> getLatencies() {
              return latencies;
          }
      }
      
      public static class PerformanceResult {
          private final int messageCount;
          private final int messageSize;
          private final int producerThreads;
          private final int consumerThreads;
          private final double producerThroughput;
          private final double consumerThroughput;
          private final long minLatency;
          private final long maxLatency;
          private final long p50Latency;
          private final long p95Latency;
          private final long p99Latency;
          
          // Constructor and getters
          // ...
          
          @Override
          public String toString() {
              return String.format(
                  "Performance Test Results:\n" +
                  "Messages: %d, Size: %d bytes\n" +
                  "Producer Threads: %d, Consumer Threads: %d\n" +
                  "Producer Throughput: %.2f msgs/sec\n" +
                  "Consumer Throughput: %.2f msgs/sec\n" +
                  "Latency (ms) - Min: %d, Max: %d, p50: %d, p95: %d, p99: %d",
                  messageCount, messageSize, producerThreads, consumerThreads,
                  producerThroughput, consumerThroughput,
                  minLatency, maxLatency, p50Latency, p95Latency, p99Latency
              );
          }
      }
  }
  ```

**4. Migration Execution**:

- **Dual Write Implementation**:
  - Configure applications to write to both systems
  - Implement consistency verification
  - Create fallback mechanisms
  
  ```java
  // Example: Dual write implementation
  
  public class DualWriteOrderService {
      private final JmsTemplate jmsTemplate;
      private final KafkaTemplate<String, String> kafkaTemplate;
      private final ObjectMapper objectMapper;
      private final String jmsQueue;
      private final String kafkaTopic;
      private final MetricsService metricsService;
      private final boolean kafkaPrimary;
      
      public DualWriteOrderService(JmsTemplate jmsTemplate,
                                 KafkaTemplate<String, String> kafkaTemplate,
                                 ObjectMapper objectMapper,
                                 String jmsQueue,
                                 String kafkaTopic,
                                 MetricsService metricsService,
                                 boolean kafkaPrimary) {
          this.jmsTemplate = jmsTemplate;
          this.kafkaTemplate = kafkaTemplate;
          this.objectMapper = objectMapper;
          this.jmsQueue = jmsQueue;
          this.kafkaTopic = kafkaTopic;
          this.metricsService = metricsService;
          this.kafkaPrimary = kafkaPrimary;
      }
      
      public void processOrder(Order order) throws OrderProcessingException {
          String orderId = order.getId();
          String orderJson;
          
          try {
              orderJson = objectMapper.writeValueAsString(order);
          } catch (JsonProcessingException e) {
              throw new OrderProcessingException("Failed to serialize order", e);
          }
          
          // Track start time for metrics
          long startTime = System.currentTimeMillis();
          
          // Determine primary and secondary systems
          MessageSystem primarySystem = kafkaPrimary ? MessageSystem.KAFKA : MessageSystem.JMS;
          MessageSystem secondarySystem = kafkaPrimary ? MessageSystem.JMS : MessageSystem.KAFKA;
          
          // Send to primary system
          boolean primarySuccess = sendToPrimary(orderId, orderJson, primarySystem);
          
          // Send to secondary system
          boolean secondarySuccess = false;
          if (primarySuccess) {
              secondarySuccess = sendToSecondary(orderId, orderJson, secondarySystem);
          }
          
          // Record metrics
          long endTime = System.currentTimeMillis();
          metricsService.recordLatency("order.processing.time", endTime - startTime);
          metricsService.incrementCounter("order.processed");
          
          if (primarySuccess) {
              metricsService.incrementCounter("order." + primarySystem.name().toLowerCase() + ".success");
          } else {
              metricsService.incrementCounter("order." + primarySystem.name().toLowerCase() + ".failure");
          }
          
          if (secondarySuccess) {
              metricsService.incrementCounter("order." + secondarySystem.name().toLowerCase() + ".success");
          } else {
              metricsService.incrementCounter("order." + secondarySystem.name().toLowerCase() + ".failure");
          }
          
          // Handle failure scenarios
          if (!primarySuccess) {
              throw new OrderProcessingException("Failed to process order in primary system: " + primarySystem);
          }
      }
      
      private boolean sendToPrimary(String orderId, String orderJson, MessageSystem system) {
          try {
              if (system == MessageSystem.KAFKA) {
                  ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, orderId, orderJson);
                  kafkaTemplate.send(record).get(5, TimeUnit.SECONDS); // Wait for confirmation
              } else {
                  jmsTemplate.send(jmsQueue, session -> {
                      TextMessage message = session.createTextMessage(orderJson);
                      message.setStringProperty("orderId", orderId);
                      return message;
                  });
              }
              return true;
          } catch (Exception e) {
              log.error("Failed to send order to primary system: {}", system, e);
              return false;
          }
      }
      
      private boolean sendToSecondary(String orderId, String orderJson, MessageSystem system) {
          try {
              if (system == MessageSystem.KAFKA) {
                  ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, orderId, orderJson);
                  record.headers().add("dual-write", "true".getBytes(StandardCharsets.UTF_8));
                  kafkaTemplate.send(record); // Don't wait for confirmation
              } else {
                  jmsTemplate.send(jmsQueue, session -> {
                      TextMessage message = session.createTextMessage(orderJson);
                      message.setStringProperty("orderId", orderId);
                      message.setBooleanProperty("dualWrite", true);
                      return message;
                  });
              }
              return true;
          } catch (Exception e) {
              log.warn("Failed to send order to secondary system: {}", system, e);
              return false;
          }
      }
      
      private enum MessageSystem {
          KAFKA, JMS
      }
  }
  ```

- **Incremental Cutover**:
  - Migrate one message flow at a time
  - Validate each migration step
  - Maintain rollback capability
  
  ```java
  // Example: Feature flag based cutover
  
  public class OrderServiceRouter {
      private final JmsOrderService jmsOrderService;
      private final KafkaOrderService kafkaOrderService;
      private final FeatureFlagService featureFlagService;
      private final DualWriteOrderService dualWriteOrderService;
      
      public OrderServiceRouter(JmsOrderService jmsOrderService,
                              KafkaOrderService kafkaOrderService,
                              DualWriteOrderService dualWriteOrderService,
                              FeatureFlagService featureFlagService) {
          this.jmsOrderService = jmsOrderService;
          this.kafkaOrderService = kafkaOrderService;
          this.dualWriteOrderService = dualWriteOrderService;
          this.featureFlagService = featureFlagService;
      }
      
      public void processOrder(Order order) throws OrderProcessingException {
          String orderId = order.getId();
          
          // Determine which implementation to use based on feature flags
          OrderProcessingMode mode = getProcessingMode(order);
          
          switch (mode) {
              case JMS_ONLY:
                  jmsOrderService.processOrder(order);
                  break;
                  
              case DUAL_WRITE_JMS_PRIMARY:
                  dualWriteOrderService.processOrder(order);
                  break;
                  
              case DUAL_WRITE_KAFKA_PRIMARY:
                  dualWriteOrderService.processOrder(order);
                  break;
                  
              case KAFKA_ONLY:
                  kafkaOrderService.processOrder(order);
                  break;
                  
              default:
                  // Default to JMS as fallback
                  jmsOrderService.processOrder(order);
          }
      }
      
      private OrderProcessingMode getProcessingMode(Order order) {
          // Check for order-specific override
          String orderTypeFlag = "order.processing.mode." + order.getOrderType().toLowerCase();
          if (featureFlagService.isFeatureFlagDefined(orderTypeFlag)) {
              return featureFlagService.getEnumValue(orderTypeFlag, OrderProcessingMode.class);
          }
          
          // Check for global setting
          return featureFlagService.getEnumValue("order.processing.mode", 
                                               OrderProcessingMode.class, 
                                               OrderProcessingMode.JMS_ONLY);
      }
      
      public enum OrderProcessingMode {
          JMS_ONLY,
          DUAL_WRITE_JMS_PRIMARY,
          DUAL_WRITE_KAFKA_PRIMARY,
          KAFKA_ONLY
      }
  }
  ```

- **Monitoring and Verification**:
  - Implement comprehensive monitoring
  - Compare message delivery in both systems
  - Track migration progress
  
  ```java
  // Example: Migration monitoring service
  
  public class MigrationMonitoringService {
      private final JdbcTemplate jdbcTemplate;
      private final MetricsService metricsService;
      private final AlertService alertService;
      
      public MigrationMonitoringService(JdbcTemplate jdbcTemplate,
                                      MetricsService metricsService,
                                      AlertService alertService) {
          this.jdbcTemplate = jdbcTemplate;
          this.metricsService = metricsService;
          this.alertService = alertService;
      }
      
      @Scheduled(fixedRate = 60000) // Run every minute
      public void monitorMigrationProgress() {
          // Check message counts
          long jmsMessageCount = getJmsMessageCount();
          long kafkaMessageCount = getKafkaMessageCount();
          
          // Record metrics
          metricsService.gauge("migration.jms.message.count", jmsMessageCount);
          metricsService.gauge("migration.kafka.message.count", kafkaMessageCount);
          
          // Calculate consistency ratio
          double consistencyRatio = calculateConsistencyRatio();
          metricsService.gauge("migration.consistency.ratio", consistencyRatio);
          
          // Check for inconsistencies
          if (consistencyRatio < 0.99) {
              alertService.sendAlert("Migration Consistency Issue", 
                                   "Consistency ratio is below threshold: " + consistencyRatio);
          }
          
          // Check processing latency
          Map<String, Double> latencyComparison = compareProcessingLatency();
          for (Map.Entry<String, Double> entry : latencyComparison.entrySet()) {
              metricsService.gauge("migration.latency.comparison." + entry.getKey(), entry.getValue());
          }
          
          // Check error rates
          Map<String, Double> errorRateComparison = compareErrorRates();
          for (Map.Entry<String, Double> entry : errorRateComparison.entrySet()) {
              metricsService.gauge("migration.error.comparison." + entry.getKey(), entry.getValue());
              
              if (entry.getValue() > 0.01) { // More than 1% error rate difference
                  alertService.sendAlert("Migration Error Rate Issue", 
                                       "Error rate difference for " + entry.getKey() + 
                                       " is " + entry.getValue());
              }
          }
          
          // Generate migration progress report
          MigrationProgressReport report = generateProgressReport();
          log.info("Migration Progress: {}", report);
      }
      
      private long getJmsMessageCount() {
          return jdbcTemplate.queryForObject(
              "SELECT COUNT(*) FROM jms_message_tracking", Long.class);
      }
      
      private long getKafkaMessageCount() {
          return jdbcTemplate.queryForObject(
              "SELECT COUNT(*) FROM kafka_message_tracking", Long.class);
      }
      
      private double calculateConsistencyRatio() {
          // Count messages that appear in both systems
          long matchingCount = jdbcTemplate.queryForObject(
              "SELECT COUNT(*) FROM jms_message_tracking j " +
              "JOIN kafka_message_tracking k ON j.correlation_id = k.correlation_id", 
              Long.class);
              
          // Count total unique messages
          long totalCount = jdbcTemplate.queryForObject(
              "SELECT COUNT(*) FROM (" +
              "  SELECT correlation_id FROM jms_message_tracking " +
              "  UNION " +
              "  SELECT correlation_id FROM kafka_message_tracking" +
              ") AS combined", 
              Long.class);
              
          return totalCount > 0 ? (double) matchingCount / totalCount : 1.0;
      }
      
      private Map<String, Double> compareProcessingLatency() {
          Map<String, Double> result = new HashMap<>();
          
          // Compare average latency for different message types
          List<Map<String, Object>> latencyData = jdbcTemplate.queryForList(
              "SELECT message_type, " +
              "AVG(jms_latency) as avg_jms_latency, " +
              "AVG(kafka_latency) as avg_kafka_latency " +
              "FROM message_latency_comparison " +
              "GROUP BY message_type");
              
          for (Map<String, Object> row : latencyData) {
              String messageType = (String) row.get("message_type");
              double jmsLatency = ((Number) row.get("avg_jms_latency")).doubleValue();
              double kafkaLatency = ((Number) row.get("avg_kafka_latency")).doubleValue();
              
              // Calculate relative difference
              double relativeDiff = jmsLatency > 0 ? 
                  (kafkaLatency - jmsLatency) / jmsLatency : 0;
                  
              result.put(messageType, relativeDiff);
          }
          
          return result;
      }
      
      private Map<String, Double> compareErrorRates() {
          Map<String, Double> result = new HashMap<>();
          
          // Compare error rates for different message types
          List<Map<String, Object>> errorData = jdbcTemplate.queryForList(
              "SELECT message_type, " +
              "SUM(CASE WHEN jms_status = 'ERROR' THEN 1 ELSE 0 END) / COUNT(*) as jms_error_rate, " +
              "SUM(CASE WHEN kafka_status = 'ERROR' THEN 1 ELSE 0 END) / COUNT(*) as kafka_error_rate " +
              "FROM message_status_comparison " +
              "GROUP BY message_type");
              
          for (Map<String, Object> row : errorData) {
              String messageType = (String) row.get("message_type");
              double jmsErrorRate = ((Number) row.get("jms_error_rate")).doubleValue();
              double kafkaErrorRate = ((Number) row.get("kafka_error_rate")).doubleValue();
              
              // Calculate absolute difference
              double absDiff = Math.abs(kafkaErrorRate - jmsErrorRate);
              result.put(messageType, absDiff);
          }
          
          return result;
      }
      
      private MigrationProgressReport generateProgressReport() {
          MigrationProgressReport report = new MigrationProgressReport();
          
          // Get overall progress
          report.setTotalMessageFlows(jdbcTemplate.queryForObject(
              "SELECT COUNT(*) FROM message_flow_registry", Integer.class));
              
          report.setCompletedMessageFlows(jdbcTemplate.queryForObject(
              "SELECT COUNT(*) FROM message_flow_registry WHERE status = 'COMPLETED'", 
              Integer.class));
              
          report.setInProgressMessageFlows(jdbcTemplate.queryForObject(
              "SELECT COUNT(*) FROM message_flow_registry WHERE status = 'IN_PROGRESS'", 
              Integer.class));
              
          report.setPendingMessageFlows(jdbcTemplate.queryForObject(
              "SELECT COUNT(*) FROM message_flow_registry WHERE status = 'PENDING'", 
              Integer.class));
              
          // Get detailed flow status
          report.setFlowDetails(jdbcTemplate.query(
              "SELECT flow_name, status, start_date, completion_date, " +
              "jms_message_count, kafka_message_count, consistency_ratio " +
              "FROM message_flow_registry",
              (rs, rowNum) -> {
                  MessageFlowStatus status = new MessageFlowStatus();
                  status.setFlowName(rs.getString("flow_name"));
                  status.setStatus(rs.getString("status"));
                  status.setStartDate(rs.getTimestamp("start_date"));
                  status.setCompletionDate(rs.getTimestamp("completion_date"));
                  status.setJmsMessageCount(rs.getLong("jms_message_count"));
                  status.setKafkaMessageCount(rs.getLong("kafka_message_count"));
                  status.setConsistencyRatio(rs.getDouble("consistency_ratio"));
                  return status;
              }
          ));
          
          return report;
      }
      
      // Report classes
      public static class MigrationProgressReport {
          private int totalMessageFlows;
          private int completedMessageFlows;
          private int inProgressMessageFlows;
          private int pendingMessageFlows;
          private List<MessageFlowStatus> flowDetails;
          
          // Getters and setters
          // ...
          
          public double getCompletionPercentage() {
              return totalMessageFlows > 0 ? 
                  (double) completedMessageFlows / totalMessageFlows * 100 : 0;
          }
      }
      
      public static class MessageFlowStatus {
          private String flowName;
          private String status;
          private Date startDate;
          private Date completionDate;
          private long jmsMessageCount;
          private long kafkaMessageCount;
          private double consistencyRatio;
          
          // Getters and setters
          // ...
      }
  }
  ```

- **Rollback Procedures**:
  - Implement rollback mechanisms
  - Test rollback procedures
  - Document rollback steps
  
  ```java
  // Example: Rollback service
  
  public class MigrationRollbackService {
      private final FeatureFlagService featureFlagService;
      private final JdbcTemplate jdbcTemplate;
      private final AlertService alertService;
      
      public MigrationRollbackService(FeatureFlagService featureFlagService,
                                    JdbcTemplate jdbcTemplate,
                                    AlertService alertService) {
          this.featureFlagService = featureFlagService;
          this.jdbcTemplate = jdbcTemplate;
          this.alertService = alertService;
      }
      
      public void rollbackMessageFlow(String flowName, String reason) {
          log.warn("Rolling back message flow: {} - Reason: {}", flowName, reason);
          
          try {
              // Update feature flags to revert to JMS
              String featureFlagKey = "order.processing.mode." + flowName.toLowerCase();
              featureFlagService.setFeatureFlag(featureFlagKey, "JMS_ONLY");
              
              // Update flow status in database
              jdbcTemplate.update(
                  "UPDATE message_flow_registry SET " +
                  "status = 'ROLLED_BACK', " +
                  "rollback_date = ?, " +
                  "rollback_reason = ? " +
                  "WHERE flow_name = ?",
                  new Timestamp(System.currentTimeMillis()),
                  reason,
                  flowName
              );
              
              // Send alert
              alertService.sendAlert("Migration Rollback", 
                                   "Message flow " + flowName + " has been rolled back: " + reason);
              
              log.info("Successfully rolled back message flow: {}", flowName);
          } catch (Exception e) {
              log.error("Failed to roll back message flow: {}", flowName, e);
              alertService.sendAlert("Migration Rollback Failed", 
                                   "Failed to roll back " + flowName + ": " + e.getMessage());
              throw new RollbackFailedException("Failed to roll back message flow: " + flowName, e);
          }
      }
      
      public void rollbackAllFlows(String reason) {
          log.warn("Rolling back all message flows - Reason: {}", reason);
          
          try {
              // Get all in-progress flows
              List<String> inProgressFlows = jdbcTemplate.queryForList(
                  "SELECT flow_name FROM message_flow_registry WHERE status = 'IN_PROGRESS'",
                  String.class
              );
              
              // Roll back each flow
              for (String flowName : inProgressFlows) {
                  rollbackMessageFlow(flowName, reason);
              }
              
              // Reset global feature flag
              featureFlagService.setFeatureFlag("order.processing.mode", "JMS_ONLY");
              
              log.info("Successfully rolled back all message flows");
              alertService.sendAlert("Complete Migration Rollback", 
                                   "All message flows have been rolled back: " + reason);
          } catch (Exception e) {
              log.error("Failed to roll back all message flows", e);
              alertService.sendAlert("Complete Migration Rollback Failed", 
                                   "Failed to roll back all flows: " + e.getMessage());
              throw new RollbackFailedException("Failed to roll back all message flows", e);
          }
      }
      
      public void automaticRollbackIfNeeded() {
          // Check for conditions that would trigger automatic rollback
          boolean shouldRollback = false;
          String rollbackReason = null;
          
          // Check error rates
          double kafkaErrorRate = getKafkaErrorRate();
          if (kafkaErrorRate > 0.05) { // More than 5% error rate
              shouldRollback = true;
              rollbackReason = "Kafka error rate too high: " + kafkaErrorRate;
          }
          
          // Check message loss
          double messageLossRate = getMessageLossRate();
          if (messageLossRate > 0.001) { // More than 0.1% message loss
              shouldRollback = true;
              rollbackReason = "Message loss rate too high: " + messageLossRate;
          }
          
          // Check latency
          double latencyIncrease = getLatencyIncrease();
          if (latencyIncrease > 2.0) { // More than 2x latency increase
              shouldRollback = true;
              rollbackReason = "Latency increase too high: " + latencyIncrease + "x";
          }
          
          // Perform rollback if needed
          if (shouldRollback) {
              log.warn("Automatic rollback triggered: {}", rollbackReason);
              rollbackAllFlows("AUTOMATIC: " + rollbackReason);
          }
      }
      
      private double getKafkaErrorRate() {
          return jdbcTemplate.queryForObject(
              "SELECT COUNT(CASE WHEN status = 'ERROR' THEN 1 END) / COUNT(*) " +
              "FROM kafka_message_tracking " +
              "WHERE timestamp > ?",
              Double.class,
              new Timestamp(System.currentTimeMillis() - 3600000) // Last hour
          );
      }
      
      private double getMessageLossRate() {
          return jdbcTemplate.queryForObject(
              "SELECT 1.0 - (SELECT COUNT(*) FROM kafka_message_tracking) / " +
              "(SELECT COUNT(*) FROM jms_message_tracking) " +
              "WHERE timestamp > ?",
              Double.class,
              new Timestamp(System.currentTimeMillis() - 3600000) // Last hour
          );
      }
      
      private double getLatencyIncrease() {
          return jdbcTemplate.queryForObject(
              "SELECT AVG(kafka_latency) / AVG(jms_latency) " +
              "FROM message_latency_comparison " +
              "WHERE timestamp > ?",
              Double.class,
              new Timestamp(System.currentTimeMillis() - 3600000) // Last hour
          );
      }
  }
  ```

**5. Post-Migration Activities**:

- **Legacy System Decommissioning**:
  - Gradually reduce legacy system capacity
  - Archive historical data if needed
  - Plan for final shutdown
  
  ```java
  // Example: Legacy system decommissioning plan
  
  public class LegacySystemDecommissionService {
      private final JdbcTemplate jdbcTemplate;
      private final FeatureFlagService featureFlagService;
      private final AlertService alertService;
      
      public void executeDecommissionPlan() {
          // Check if all flows are migrated
          boolean allFlowsMigrated = areAllFlowsMigrated();
          
          if (!allFlowsMigrated) {
              log.warn("Cannot proceed with decommissioning - not all flows are migrated");
              return;
          }
          
          // Step 1: Archive historical data
          archiveHistoricalData();
          
          // Step 2: Reduce capacity
          reduceCapacity();
          
          // Step 3: Disable write path
          disableWritePath();
          
          // Step 4: Verify no new messages
          if (verifyNoNewMessages()) {
              // Step 5: Disable read path
              disableReadPath();
              
              // Step 6: Final shutdown
              scheduleShutdown();
          } else {
              // Abort decommissioning
              log.error("Decommissioning aborted - still receiving messages");
              alertService.sendAlert("Decommissioning Aborted", 
                                   "Legacy system is still receiving messages");
          }
      }
      
      private boolean areAllFlowsMigrated() {
          Integer pendingFlows = jdbcTemplate.queryForObject(
              "SELECT COUNT(*) FROM message_flow_registry " +
              "WHERE status NOT IN ('COMPLETED', 'SKIPPED')",
              Integer.class
          );
          
          return pendingFlows != null && pendingFlows == 0;
      }
      
      private void archiveHistoricalData() {
          log.info("Archiving historical data from legacy system");
          
          // Archive data older than 90 days
          int archivedCount = jdbcTemplate.update(
              "INSERT INTO message_archive (message_id, queue_name, message_data, created_date) " +
              "SELECT message_id, queue_name, message_data, created_date " +
              "FROM jms_messages " +
              "WHERE created_date < ?",
              new Timestamp(System.currentTimeMillis() - 90 * 24 * 3600 * 1000L)
          );
          
          log.info("Archived {} messages from legacy system", archivedCount);
          
          // Delete archived data
          int deletedCount = jdbcTemplate.update(
              "DELETE FROM jms_messages " +
              "WHERE created_date < ?",
              new Timestamp(System.currentTimeMillis() - 90 * 24 * 3600 * 1000L)
          );
          
          log.info("Deleted {} archived messages from legacy system", deletedCount);
      }
      
      private void reduceCapacity() {
          log.info("Reducing legacy system capacity");
          
          // Update broker configuration
          jdbcTemplate.update(
              "UPDATE jms_broker_config " +
              "SET max_connections = max_connections / 2, " +
              "    max_memory = max_memory / 2, " +
              "    max_queue_size = max_queue_size / 2"
          );
          
          // Restart brokers with new configuration
          restartBrokersWithReducedCapacity();
      }
      
      private void restartBrokersWithReducedCapacity() {
          // Implementation depends on specific JMS broker
          // ...
      }
      
      private void disableWritePath() {
          log.info("Disabling write path to legacy system");
          
          // Set feature flag to disable JMS writes
          featureFlagService.setFeatureFlag("jms.write.enabled", "false");
          
          // Update all message flows to Kafka-only mode
          jdbcTemplate.update(
              "UPDATE message_flow_registry " +
              "SET status = 'COMPLETED', " +
              "    completion_date = ? " +
              "WHERE status = 'IN_PROGRESS'",
              new Timestamp(System.currentTimeMillis())
          );
          
          // Set global processing mode to Kafka-only
          featureFlagService.setFeatureFlag("order.processing.mode", "KAFKA_ONLY");
          
          // Send alert
          alertService.sendAlert("Legacy System Write Path Disabled", 
                               "Write path to JMS system has been disabled");
      }
      
      private boolean verifyNoNewMessages() {
          log.info("Verifying no new messages in legacy system");
          
          // Wait for a period to ensure no new messages
          try {
              Thread.sleep(3600000); // Wait 1 hour
          } catch (InterruptedException e) {
              Thread.currentThrea
package io.taskscheduler.worker;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

/** Standalone contract driver: instantiate a Java task DTO and optionally publish it to Kafka. */
public final class ContractProbe {
  private ContractProbe() {}

  public static void main(String[] args) throws Exception {
    Wire.TaskMessage source =
        Wire.task(new String(System.in.readAllBytes(), StandardCharsets.UTF_8));
    Wire.TaskMessage message =
        new Wire.TaskMessage(
            source.version(),
            source.aiModified(),
            source.generation(),
            source.taskId(),
            source.type(),
            source.payload().deepCopy(),
            source.priority(),
            source.attemptNo(),
            source.maxRetries(),
            source.scheduledAt(),
            source.createdAt());
    String encoded = Wire.encode(message);
    if (args.length > 0 && args[0].equals("emit")) {
      Properties settings = new Properties();
      settings.put(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
          System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
      settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      settings.put(ProducerConfig.ACKS_CONFIG, "all");
      settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      settings.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
      settings.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15000);
      try (KafkaProducer<String, String> producer = new KafkaProducer<>(settings)) {
        String key = Wire.key(message.taskId(), message.priority());
        ProducerRecord<String, String> record =
            new ProducerRecord<>("tasks.pending", Wire.partition(key), key, encoded);
        record.headers().add("producer-language", "java".getBytes(StandardCharsets.UTF_8));
        try (Correlation ignored = Correlation.bind(message.taskId().toString(), System.getenv("TASK_TRACE_ID"))) {
          record.headers().add("task_id", message.taskId().toString().getBytes(StandardCharsets.UTF_8));
          record.headers().add("trace_id", org.slf4j.MDC.get("trace_id").getBytes(StandardCharsets.US_ASCII));
        }
        producer.send(record).get(20, TimeUnit.SECONDS);
        producer.close(Duration.ofSeconds(2));
      }
    }
    System.out.println(encoded);
  }
}

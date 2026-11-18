package io.taskscheduler.worker;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** Both groups use manual immediate acknowledgements on the consumer thread. */
@Component
public class KafkaListeners {
  private final WorkerRuntime runtime;
  private final WorkerStore store;

  public KafkaListeners(WorkerRuntime runtime, WorkerStore store) {
    this.runtime = runtime;
    this.store = store;
  }

  @KafkaListener(
      id = "java-pending",
      topics = "tasks.pending",
      groupId = "${worker.group-id}",
      concurrency = "${worker.concurrency}")
  public void pending(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
    JsonNode routing = Wire.JSON.readTree(record.value());
    if (routing != null
        && routing.path("type").isTextual()
        && !routing.path("type").textValue().strip().startsWith("java.")) {
      ack.acknowledge();
      return;
    }
    Wire.TaskMessage message = Wire.task(record.value());
    try (Correlation ignored = Correlation.bind(record, message.taskId().toString())) {
      if (runtime.process(message)) ack.acknowledge();
      else ack.nack(Duration.ofSeconds(1));
    }
  }

  @KafkaListener(
      id = "java-retries",
      topics = {"tasks.retry.5s", "tasks.retry.30s", "tasks.retry.5m"},
      groupId = "${worker.group-id}.retries")
  public void retry(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
    JsonNode data = Wire.JSON.valueToTree(Wire.retry(record.value()));
    try (Correlation ignored = Correlation.bind(record, data.path("task_id").asText())) {
      store.ingestRetry(data);
      ack.acknowledge();
    }
  }
}

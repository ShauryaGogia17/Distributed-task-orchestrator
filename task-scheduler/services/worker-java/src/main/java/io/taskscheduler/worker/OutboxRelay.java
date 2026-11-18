package io.taskscheduler.worker;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.persistence.*;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Shared SKIP LOCKED transactional relay, interoperable with the Python publishers. */
@Component
public class OutboxRelay {
  private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
  @PersistenceContext private EntityManager em;
  private final TransactionTemplate tx;
  private final KafkaTemplate<String, String> kafka;
  private final WorkerProperties properties;
  private final MeterRegistry metrics;

  public OutboxRelay(
      PlatformTransactionManager manager,
      KafkaTemplate<String, String> kafka,
      WorkerProperties properties,
      MeterRegistry metrics) {
    tx = new TransactionTemplate(manager);
    this.kafka = kafka;
    this.properties = properties;
    this.metrics = metrics;
  }

  @Scheduled(fixedDelayString = "${JAVA_WORKER_OUTBOX_POLL_MS:250}")
  public void publish() {
    try {
      tx.executeWithoutResult(
          status -> {
            @SuppressWarnings("unchecked")
            List<Rows.Outbox> rows =
                em.createNativeQuery(
                        "select * from outbox where published_at is null order by created_at,id limit :limit for update skip locked",
                        Rows.Outbox.class)
                    .setParameter("limit", properties.outboxBatchSize())
                    .getResultList();
            for (Rows.Outbox row : rows) {
              Integer partition =
                  row.topic.equals("tasks.pending") ? Wire.partition(row.key) : null;
              ProducerRecord<String, String> record =
                  new ProducerRecord<>(row.topic, partition, row.key, Wire.encode(row.payload));
              record
                  .headers()
                  .add("outbox-id", row.id.toString().getBytes(StandardCharsets.US_ASCII));
              record
                  .headers()
                  .add(
                      "trace_id",
                      row.traceId.toString().replace("-", "").getBytes(StandardCharsets.US_ASCII));
              if (row.payload.hasNonNull("task_id"))
                record
                    .headers()
                    .add(
                        "task_id",
                        row.payload.path("task_id").asText().getBytes(StandardCharsets.UTF_8));
              try (Correlation ignored =
                  Correlation.bind(row.payload.path("task_id").asText(), row.traceId.toString())) {
                kafka.send(record).get(properties.publishTimeoutSeconds(), TimeUnit.SECONDS);
                log.info("Outbox published topic={}", row.topic);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Publication interrupted", e);
              } catch (Exception e) {
                throw new IllegalStateException("Publication not acknowledged", e);
              }
              row.publishedAt = OffsetDateTime.now(ZoneOffset.UTC);
              if (row.topic.equals("tasks.dlq")
                  && row.payload.path("kind").asText().equals("task.dead_letter")) {
                Rows.Task task =
                    em.find(
                        Rows.Task.class,
                        UUID.fromString(row.payload.path("task_id").asText()),
                        LockModeType.PESSIMISTIC_WRITE);
                if (task != null
                    && task.generation == row.payload.path("generation").asInt(0)
                    && task.attempts == row.payload.path("attempt_no").asInt()
                    && task.status.equals("FAILED")) task.status = "DEAD_LETTERED";
              }
              metrics.counter("java_worker_outbox_published", "topic", row.topic).increment();
            }
          });
    } catch (Exception e) {
      log.warn("Outbox transaction retained for retry: {}", e.getClass().getSimpleName());
    }
  }
}

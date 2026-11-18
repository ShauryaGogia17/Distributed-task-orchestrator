package io.taskscheduler.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import jakarta.persistence.*;
import java.io.*;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Short JPA transactions fence claims/outcomes; handlers and Kafka polling run outside them. */
@Repository
public class WorkerStore {
  @PersistenceContext private EntityManager em;
  private final TransactionTemplate tx;
  private final WorkerProperties properties;
  private final RedisState redis;
  private static final Set<String> FINISHED =
      Set.of("SUCCEEDED", "FAILED", "CANCELLED", "DEAD_LETTERED", "AI_DIAGNOSED", "ESCALATED");

  public record Execution(
      UUID taskId,
      String type,
      JsonNode payload,
      int generation,
      int attemptNo,
      String token,
      RedisState.Permit permit,
      UUID traceId) {}

  public record Claim(boolean ready, Execution execution, List<UUID> events) {}

  public static class LeaseLost extends RuntimeException {
    public LeaseLost() {
      super("Execution lease/generation no longer owned");
    }
  }

  public WorkerStore(
      PlatformTransactionManager manager, WorkerProperties properties, RedisState redis) {
    this.tx = new TransactionTemplate(manager);
    this.properties = properties;
    this.redis = redis;
  }

  private OffsetDateTime now() {
    BigDecimal seconds =
        new BigDecimal(
            em.createNativeQuery("select extract(epoch from clock_timestamp())")
                .getSingleResult()
                .toString());
    return OffsetDateTime.ofInstant(
        Instant.ofEpochSecond(
            seconds.longValue(), seconds.remainder(BigDecimal.ONE).movePointRight(9).longValue()),
        ZoneOffset.UTC);
  }

  private Rows.Task locked(UUID id) {
    return em.find(Rows.Task.class, id, LockModeType.PESSIMISTIC_WRITE);
  }

  private Rows.Attempt attempt(Rows.Task task) {
    return em.find(Rows.Attempt.class, new Rows.AttemptId(task.id, task.generation, task.attempts));
  }

  private List<UUID> completion(Rows.Task task, int number) {
    List<UUID> ids =
        List.of(
            Wire.eventId(task.id, number, "result", task.generation),
            Wire.eventId(task.id, number, "retry", task.generation),
            Wire.eventId(task.id, number, "dlq", task.generation));
    return em.createQuery("select o.id from OutboxRow o where o.id in :ids", UUID.class)
        .setParameter("ids", ids)
        .getResultList();
  }

  private Claim done(List<UUID> events) {
    return new Claim(true, null, events);
  }

  private void park(Rows.Task task, OffsetDateTime due) {
    if (task.status.equals("PENDING")) task.status = "SCHEDULED";
    task.scheduledAt = due;
    em.createNativeQuery(
            "insert into delayed_tasks(task_id,release_at) values (:id,:due) on conflict(task_id) do update set release_at=excluded.release_at,dispatched_at=null")
        .setParameter("id", task.id)
        .setParameter("due", due)
        .executeUpdate();
  }

  public Claim claim(Wire.TaskMessage message, String token) {
    return tx.execute(
        status -> {
          Rows.Task task = locked(message.taskId());
          if (task == null || !task.type.startsWith("java.")) return done(List.of());
          if (message.generation() != task.generation)
            return message.generation() < task.generation
                ? done(List.of())
                : new Claim(false, null, List.of());
          if (FINISHED.contains(task.status)) return done(completion(task, task.attempts));
          Number delayed =
              (Number)
                  em.createNativeQuery(
                          "select count(*) from delayed_tasks where task_id=:id and dispatched_at is null")
                      .setParameter("id", task.id)
                      .getSingleResult();
          if (delayed.longValue() > 0) return done(List.of());
          if (message.attemptNo() < task.attempts
              || (message.attemptNo() <= task.attempts && !task.status.equals("RUNNING")))
            return done(completion(task, message.attemptNo()));
          if (!Set.of("PENDING", "SCHEDULED", "RUNNING").contains(task.status))
            return new Claim(false, null, List.of());
          if (!redis.owns(task.id, task.generation, token)) throw new LeaseLost();
          OffsetDateTime current = now();
          if (task.status.equals("RUNNING")) return done(crashed(task, current));
          if (message.attemptNo() != task.attempts + 1) return new Claim(false, null, List.of());
          if (task.scheduledAt.isAfter(current)) {
            park(task, task.scheduledAt);
            return done(List.of());
          }
          RedisState.Permit permit = redis.circuit(task.type);
          if (!permit.allowed()) {
            park(task, current.plusNanos((long) (permit.retryAfterSeconds() * 1e9)));
            return done(List.of());
          }
          task.status = "RUNNING";
          task.attempts++;
          task.lastError = null;
          Rows.Attempt attempt = new Rows.Attempt();
          attempt.taskId = task.id;
          attempt.generation = task.generation;
          attempt.attemptNo = task.attempts;
          attempt.workerId = properties.id();
          attempt.startedAt = current;
          em.persist(attempt);
          return new Claim(
              true,
              new Execution(
                  task.id,
                  task.type,
                  task.payload.deepCopy(),
                  task.generation,
                  task.attempts,
                  token,
                  permit,
                  task.traceId),
              List.of());
        });
  }

  public List<UUID> finish(Execution execution, JsonNode result, Exception error) {
    return tx.execute(
        status -> {
          Rows.Task task = locked(execution.taskId());
          if (task == null
              || task.generation != execution.generation()
              || task.attempts != execution.attemptNo()
              || !task.status.equals("RUNNING")
              || !redis.owns(task.id, task.generation, execution.token())) throw new LeaseLost();
          Rows.Attempt attempt = attempt(task);
          if (attempt == null
              || attempt.finishedAt != null
              || !attempt.workerId.equals(properties.id())) throw new LeaseLost();
          OffsetDateTime current = now();
          attempt.finishedAt = current;
          if (error != null) {
            attempt.error = truncate(error.toString(), 8000);
            StringWriter buffer = new StringWriter();
            error.printStackTrace(new PrintWriter(buffer));
            String trace = buffer.toString();
            attempt.stackTrace = trace.substring(Math.max(0, trace.length() - 32000));
            return failure(task, attempt, error, current, false);
          }
          task.status = "SUCCEEDED";
          task.result = result;
          task.lastError = null;
          return List.of(result(task, current, properties.id()));
        });
  }

  private String truncate(String value, int size) {
    return value.length() <= size ? value : value.substring(0, size);
  }

  private UUID outbox(UUID id, String topic, String key, JsonNode payload, OffsetDateTime current) {
    Rows.Outbox row = new Rows.Outbox();
    row.id = id;
    row.topic = topic;
    row.key = key;
    row.payload = payload;
    row.createdAt = current;
    em.persist(row);
    return id;
  }

  private UUID result(Rows.Task task, OffsetDateTime current, String workerId) {
    ObjectNode data =
        Wire.object()
            .put("version", 1)
            .put("generation", task.generation)
            .put("task_id", task.id.toString())
            .put("type", task.type)
            .put("status", task.status)
            .put("attempt_no", task.attempts)
            .put("worker_id", workerId)
            .put("finished_at", current.toString());
    Rows.Attempt measured = attempt(task);
    if (measured != null && measured.startedAt != null)
      data.put(
          "duration_seconds",
          Math.max(0, Duration.between(measured.startedAt, current).toNanos() / 1e9));
    data.set("result", task.result == null ? NullNode.instance : task.result);
    data.put("error", task.lastError);
    return outbox(
        Wire.eventId(task.id, task.attempts, "result", task.generation),
        "tasks.results",
        task.id.toString(),
        data,
        current);
  }

  private List<UUID> failure(
      Rows.Task task,
      Rows.Attempt attempt,
      Exception error,
      OffsetDateTime current,
      boolean crashed) {
    em.flush();
    List<Rows.Attempt> attempts =
        em.createQuery(
                "select a from AttemptRow a where a.taskId=:id and a.generation=:generation order by a.attemptNo",
                Rows.Attempt.class)
            .setParameter("id", task.id)
            .setParameter("generation", task.generation)
            .getResultList();
    int streak = 0;
    for (int i = attempts.size() - 1;
        i >= 0 && attempts.get(i).error != null && attempts.get(i).error.startsWith("WorkerCrash:");
        i--) streak++;
    boolean fatal = error instanceof Handlers.Fatal || error instanceof IllegalArgumentException;
    boolean exhausted = task.attempts >= Math.max(1, task.maxRetries);
    boolean poisoned = crashed && streak >= properties.maxCrashClaims();
    task.result = null;
    task.lastError = attempt.error;
    UUID decision;
    if (fatal || exhausted || poisoned) {
      task.status = "FAILED";
      String reason =
          poisoned
              ? "poison pill: repeated worker crashes"
              : fatal ? "fatal error" : "attempt budget exhausted";
      String category = poisoned ? "poison" : fatal ? "fatal" : "exhausted";
      ArrayNode history = Wire.JSON.createArrayNode();
      for (Rows.Attempt item : attempts)
        if (item.error != null)
          history.add(
              Wire.object()
                  .put("attempt_no", item.attemptNo)
                  .put("generation", item.generation)
                  .put("worker_id", item.workerId)
                  .put("error", item.error)
                  .put("stack_trace", item.stackTrace)
                  .put(
                      "recorded_at",
                      (item.finishedAt == null ? current : item.finishedAt).toString()));
      decision = Wire.eventId(task.id, task.attempts, "dlq", task.generation);
      Rows.DeadLetter dlq = new Rows.DeadLetter();
      dlq.id = decision;
      dlq.taskId = task.id;
      dlq.generation = task.generation;
      dlq.reason = reason;
      dlq.errorHistory = history;
      dlq.createdAt = current;
      dlq.taskType = task.type;
      dlq.workerId = attempt.workerId;
      dlq.attempts = task.attempts;
      dlq.category = category;
      dlq.failureSignature = Fingerprint.signature(attempt.error, attempt.stackTrace);
      em.persist(dlq);
      ObjectNode data =
          Wire.object()
              .put("version", 1)
              .put("kind", "task.dead_letter")
              .put("generation", task.generation)
              .put("worker_id", attempt.workerId)
              .put("category", category)
              .put("id", decision.toString())
              .put("task_id", task.id.toString())
              .put("type", task.type)
              .put("attempt_no", task.attempts)
              .put("status", "DEAD_LETTERED")
              .put("reason", reason)
              .put("created_at", current.toString());
      data.set("error_history", history);
      data.putNull("ai_diagnosis");
      data.putNull("resolution");
      outbox(decision, "tasks.dlq", task.id.toString(), data, current);
    } else {
      task.status = "RETRYING";
      double window =
          Math.min(
              properties.retryCapSeconds(),
              properties.retryBaseSeconds() * Math.pow(2, Math.min(100, task.attempts - 1)));
      double delay = window == 0 ? 0 : ThreadLocalRandom.current().nextDouble(window);
      OffsetDateTime due = current.plusNanos((long) (delay * 1e9));
      // Postgres timestamps have microsecond precision, identical to Python datetime.
      due = due.withNano((due.getNano() / 1000) * 1000);
      task.scheduledAt = due;
      decision = Wire.eventId(task.id, task.attempts, "retry", task.generation);
      Rows.Retry retry = new Rows.Retry();
      retry.id = decision;
      retry.taskId = task.id;
      retry.generation = task.generation;
      retry.failedAttempt = task.attempts;
      retry.dueAt = due;
      retry.createdAt = current;
      em.persist(retry);
      ObjectNode data =
          Wire.object()
              .put("version", 1)
              .put("generation", task.generation)
              .put("event_id", decision.toString())
              .put("task_id", task.id.toString())
              .put("failed_attempt", task.attempts)
              .put("due_at", due.toString())
              .put("delay_seconds", delay);
      outbox(decision, Wire.retryTopic(task.attempts), task.id.toString(), data, current);
    }
    return List.of(result(task, current, attempt.workerId), decision);
  }

  private List<UUID> crashed(Rows.Task task, OffsetDateTime current) {
    Rows.Attempt attempt = attempt(task);
    if (attempt == null || attempt.finishedAt != null)
      throw new IllegalStateException("RUNNING task lacks an unfinished attempt");
    attempt.finishedAt = current;
    attempt.error = "WorkerCrash: visibility lease expired without completion";
    attempt.stackTrace = "Unavailable: worker terminated before recording a traceback";
    return failure(task, attempt, new Handlers.Retryable(attempt.error), current, true);
  }

  public boolean published(List<UUID> ids) {
    if (ids.isEmpty()) return true;
    return Boolean.TRUE.equals(
        tx.execute(
            status ->
                em.createQuery(
                            "select count(o) from OutboxRow o where o.id in :ids and o.publishedAt is null",
                            Long.class)
                        .setParameter("ids", ids)
                        .getSingleResult()
                    == 0));
  }

  public void recoverExpired() {
    tx.executeWithoutResult(
        status -> {
          @SuppressWarnings("unchecked")
          List<Rows.Task> tasks =
              em.createNativeQuery(
                      "select * from tasks where status='RUNNING' and type like 'java.%' order by updated_at limit 50 for update skip locked",
                      Rows.Task.class)
                  .getResultList();
          for (Rows.Task task : tasks)
            if (!redis.exists(task.id, task.generation)) crashed(task, now());
        });
  }

  public void ingestRetry(JsonNode data) {
    tx.executeWithoutResult(
        status -> {
          UUID id = UUID.fromString(data.path("event_id").asText());
          Rows.Retry row = em.find(Rows.Retry.class, id, LockModeType.PESSIMISTIC_WRITE);
          if (row == null) return;
          Rows.Task task = em.find(Rows.Task.class, row.taskId);
          if (task == null || !task.type.startsWith("java.")) return;
          if (!Wire.eventId(row.taskId, row.failedAttempt, "retry", row.generation).equals(id)
              || !row.taskId.toString().equals(data.path("task_id").asText())
              || row.generation != data.path("generation").asInt(0)
              || row.failedAttempt != data.path("failed_attempt").asInt()
              || !row.dueAt
                  .toInstant()
                  .equals(OffsetDateTime.parse(data.path("due_at").asText()).toInstant()))
            throw new IllegalArgumentException("Retry does not match durable inbox");
          if (row.receivedAt == null) row.receivedAt = now();
        });
  }

  public void dispatchRetries() {
    tx.executeWithoutResult(
        status -> {
          @SuppressWarnings("unchecked")
          List<Rows.Retry> rows =
              em.createNativeQuery(
                      "select * from retry_schedules where received_at is not null and dispatched_at is null and due_at<=clock_timestamp() order by due_at,id limit 50 for update skip locked",
                      Rows.Retry.class)
                  .getResultList();
          for (Rows.Retry row : rows) {
            Rows.Task task = locked(row.taskId);
            if (task != null
                && task.generation == row.generation
                && task.status.equals("RETRYING")
                && task.attempts == row.failedAttempt) {
              task.status = "SCHEDULED";
              Wire.TaskMessage message =
                  new Wire.TaskMessage(
                      1,
                      task.aiModified,
                      task.generation,
                      task.id,
                      task.type,
                      task.payload,
                      task.priority,
                      task.attempts + 1,
                      task.maxRetries,
                      row.dueAt,
                      task.createdAt);
              outbox(
                  Wire.eventId(task.id, task.attempts, "redispatch", task.generation),
                  "tasks.pending",
                  Wire.key(task.id, task.priority),
                  Wire.JSON.valueToTree(message),
                  now());
            }
            row.dispatchedAt = now();
          }
        });
  }
}

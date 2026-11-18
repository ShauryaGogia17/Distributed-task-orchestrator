package io.taskscheduler.worker;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** JPA mappings of the existing Alembic-owned schema; Hibernate never creates tables. */
public final class Rows {
  private Rows() {}

  @Entity(name = "TaskRow")
  @Table(name = "tasks")
  public static class Task {
    @Id public UUID id;

    @Column(insertable = false, updatable = false)
    public UUID traceId;

    @Column(length = 128, nullable = false)
    public String type;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb", nullable = false)
    public JsonNode payload;

    @Column(length = 32, nullable = false)
    public String status;

    public int priority;
    public int attempts;
    public int generation;
    public int maxRetries;
    public boolean aiModified;
    public int aiRequeues;
    public OffsetDateTime scheduledAt;

    @Column(insertable = false, updatable = false)
    public OffsetDateTime createdAt;

    @Column(insertable = false, updatable = false)
    public OffsetDateTime updatedAt;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    public JsonNode result;

    @Column(columnDefinition = "text")
    public String lastError;
  }

  public static class AttemptId implements Serializable {
    public UUID taskId;
    public int generation;
    public int attemptNo;

    public AttemptId() {}

    public AttemptId(UUID taskId, int generation, int attemptNo) {
      this.taskId = taskId;
      this.generation = generation;
      this.attemptNo = attemptNo;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof AttemptId a
          && Objects.equals(taskId, a.taskId)
          && generation == a.generation
          && attemptNo == a.attemptNo;
    }

    @Override
    public int hashCode() {
      return Objects.hash(taskId, generation, attemptNo);
    }
  }

  @Entity(name = "AttemptRow")
  @Table(name = "task_attempts")
  @IdClass(AttemptId.class)
  public static class Attempt {
    @Id public UUID taskId;
    @Id public int generation;
    @Id public int attemptNo;

    @Column(length = 255, nullable = false)
    public String workerId;

    public OffsetDateTime startedAt;
    public OffsetDateTime finishedAt;

    @Column(columnDefinition = "text")
    public String error;

    @Column(columnDefinition = "text")
    public String stackTrace;
  }

  @Entity(name = "OutboxRow")
  @Table(name = "outbox")
  public static class Outbox {
    @Id public UUID id;

    @Column(insertable = false, updatable = false)
    public UUID traceId;

    @Column(length = 249, nullable = false)
    public String topic;

    @Column(length = 255, nullable = false)
    public String key;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb", nullable = false)
    public JsonNode payload;

    public OffsetDateTime publishedAt;
    public OffsetDateTime createdAt;
  }

  @Entity(name = "RetryRow")
  @Table(name = "retry_schedules")
  public static class Retry {
    @Id public UUID id;
    public UUID taskId;
    public int generation;
    public int failedAttempt;
    public OffsetDateTime dueAt;
    public OffsetDateTime receivedAt;
    public OffsetDateTime dispatchedAt;
    public OffsetDateTime createdAt;
  }

  @Entity(name = "DeadLetterRow")
  @Table(name = "dead_letters")
  public static class DeadLetter {
    @Id public UUID id;

    @Column(insertable = false, updatable = false)
    public UUID traceId;

    public UUID taskId;

    @Column(columnDefinition = "text", nullable = false)
    public String reason;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb", nullable = false)
    public JsonNode errorHistory;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    public JsonNode aiDiagnosis;

    @Column(length = 32)
    public String resolution;

    public OffsetDateTime createdAt;
    public int generation;

    @Column(length = 128, nullable = false)
    public String taskType;

    @Column(length = 255)
    public String workerId;

    public int attempts;

    @Column(length = 32, nullable = false)
    public String category;

    @Column(length = 16, nullable = false)
    public String status = "OPEN";

    @Column(length = 64)
    public String failureSignature;

    public OffsetDateTime receivedAt;
    public boolean agentEligible = true;
  }
}

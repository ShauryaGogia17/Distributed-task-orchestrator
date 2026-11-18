package io.taskscheduler.worker;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

/** Version-one JSON contract, deterministic UUIDv5 outcomes and priority routing. */
public final class Wire {
  public static final ObjectMapper JSON =
      JsonMapper.builder()
          .addModule(new JavaTimeModule())
          .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
          .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
          .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .build();
  private static final UUID URL_NAMESPACE = UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8");

  private Wire() {}

  /** Missing optional fields have the same defaults as shared.models.TaskMessage. */
  public record TaskMessage(
      Integer version,
      Boolean aiModified,
      Integer generation,
      UUID taskId,
      String type,
      JsonNode payload,
      Integer priority,
      Integer attemptNo,
      Integer maxRetries,
      OffsetDateTime scheduledAt,
      OffsetDateTime createdAt) {
    public TaskMessage {
      version = version == null ? 1 : version;
      aiModified = aiModified == null ? false : aiModified;
      generation = generation == null ? 0 : generation;
      payload = payload == null ? JSON.createObjectNode() : payload;
      priority = priority == null ? 0 : priority;
      attemptNo = attemptNo == null ? 1 : attemptNo;
      maxRetries = maxRetries == null ? 3 : maxRetries;
      createdAt = createdAt == null ? OffsetDateTime.now(ZoneOffset.UTC) : createdAt;
      if (type != null) type = type.strip();
      if (taskId == null
          || type == null
          || !type.matches("[a-zA-Z][a-zA-Z0-9_.-]{0,127}")
          || scheduledAt == null
          || version != 1
          || generation < 0
          || priority < 0
          || priority > 9
          || attemptNo < 1
          || maxRetries < 0
          || maxRetries > 100
          || !payload.isObject()) {
        throw new IllegalArgumentException("Invalid version-one task envelope");
      }
    }
  }

  public record RetryMessage(
      Integer version,
      Integer generation,
      UUID eventId,
      UUID taskId,
      Integer failedAttempt,
      OffsetDateTime dueAt,
      Double delaySeconds) {
    public RetryMessage {
      version = version == null ? 1 : version;
      generation = generation == null ? 0 : generation;
      if (version != 1
          || generation < 0
          || eventId == null
          || taskId == null
          || failedAttempt == null
          || failedAttempt < 1
          || dueAt == null
          || delaySeconds == null
          || !Double.isFinite(delaySeconds)
          || delaySeconds < 0)
        throw new IllegalArgumentException("Invalid version-one retry envelope");
    }
  }

  private static JsonNode root(String value) throws Exception {
    JsonNode root = JSON.readTree(value);
    if (root == null || !root.isObject())
      throw new IllegalArgumentException("Object envelope required");
    for (var iterator = root.fields(); iterator.hasNext(); )
      if (iterator.next().getValue().isNull())
        throw new IllegalArgumentException("Envelope fields cannot be null");
    return root;
  }

  public static TaskMessage task(String value) {
    try {
      return JSON.treeToValue(root(value), TaskMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid task JSON/schema", e);
    }
  }

  public static RetryMessage retry(String value) {
    try {
      return JSON.treeToValue(root(value), RetryMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid retry JSON/schema", e);
    }
  }

  public static String encode(Object value) {
    try {
      return JSON.writeValueAsString(value);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot serialize wire value", e);
    }
  }

  public static ObjectNode object() {
    return JSON.createObjectNode();
  }

  public static UUID eventId(UUID taskId, int attempt, String kind, int generation) {
    String text =
        "task-scheduler:"
            + kind
            + ":"
            + taskId
            + ":"
            + attempt
            + (generation == 0 ? "" : ":generation:" + generation);
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-1");
      digest.update(
          ByteBuffer.allocate(16)
              .putLong(URL_NAMESPACE.getMostSignificantBits())
              .putLong(URL_NAMESPACE.getLeastSignificantBits())
              .array());
      byte[] bytes = Arrays.copyOf(digest.digest(text.getBytes(StandardCharsets.UTF_8)), 16);
      bytes[6] = (byte) ((bytes[6] & 0x0f) | 0x50);
      bytes[8] = (byte) ((bytes[8] & 0x3f) | 0x80);
      ByteBuffer data = ByteBuffer.wrap(bytes);
      return new UUID(data.getLong(), data.getLong());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static String key(UUID taskId, int priority) {
    return (priority >= 7 ? "high" : priority >= 3 ? "normal" : "low") + ":" + taskId;
  }

  public static int partition(String key) {
    String[] parts = key.split(":", 2);
    if (parts.length != 2) throw new IllegalArgumentException("Invalid priority key");
    int base =
        switch (parts[0]) {
          case "high" -> 0;
          case "normal" -> 2;
          case "low" -> 4;
          default -> throw new IllegalArgumentException("Invalid priority lane");
        };
    UUID id = UUID.fromString(parts[1]);
    try {
      byte[] hash =
          MessageDigest.getInstance("SHA-256")
              .digest(
                  ByteBuffer.allocate(16)
                      .putLong(id.getMostSignificantBits())
                      .putLong(id.getLeastSignificantBits())
                      .array());
      return base + (Byte.toUnsignedInt(hash[0]) % 2);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static String retryTopic(int attempt) {
    if (attempt < 1) throw new IllegalArgumentException("Attempt must be positive");
    return attempt == 1 ? "tasks.retry.5s" : attempt == 2 ? "tasks.retry.30s" : "tasks.retry.5m";
  }
}

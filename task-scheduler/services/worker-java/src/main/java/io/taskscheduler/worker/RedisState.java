package io.taskscheduler.worker;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

/** Shared token leases and generated Lua implement the exact Python Redis protocol. */
@Component
public class RedisState {
  private final StringRedisTemplate redis;
  private final WorkerProperties properties;

  public record Permit(boolean allowed, String epoch, String token, double retryAfterSeconds) {}

  public RedisState(StringRedisTemplate redis, WorkerProperties properties) {
    this.redis = redis;
    this.properties = properties;
  }

  private <T> DefaultRedisScript<T> script(String name, Class<T> resultType) {
    DefaultRedisScript<T> value = new DefaultRedisScript<>();
    value.setLocation(new ClassPathResource("lua/" + name + ".lua"));
    value.setResultType(resultType);
    return value;
  }

  private DefaultRedisScript<Long> script(String name) {
    return script(name, Long.class);
  }

  public String leaseKey(UUID id, int generation) {
    return "task:lease:" + id + (generation == 0 ? "" : ":g" + generation);
  }

  public boolean acquire(UUID id, int generation, String token) {
    return Boolean.TRUE.equals(
        redis
            .opsForValue()
            .setIfAbsent(leaseKey(id, generation), token, Duration.ofMillis(properties.leaseMs())));
  }

  public boolean owns(UUID id, int generation, String token) {
    return token.equals(redis.opsForValue().get(leaseKey(id, generation)));
  }

  public boolean exists(UUID id, int generation) {
    return Boolean.TRUE.equals(redis.hasKey(leaseKey(id, generation)));
  }

  public void release(UUID id, int generation, String token) {
    redis.execute(script("release"), List.of(leaseKey(id, generation)), token);
  }

  public boolean renew(UUID id, int generation, String token) {
    return Long.valueOf(1)
        .equals(
            redis.execute(
                script("renew"),
                List.of(leaseKey(id, generation)),
                token,
                Long.toString(properties.leaseMs())));
  }

  public void completed(UUID id, int generation) {
    redis
        .opsForValue()
        .set(
            "task:completed:" + id + (generation == 0 ? "" : ":g" + generation),
            "SUCCEEDED",
            Duration.ofDays(1));
  }

  private String circuitKey(String type) {
    try {
      return properties.circuitKeyPrefix()
          + HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(type.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public Permit circuit(String type) {
    DefaultRedisScript<List> script = script("circuit_acquire", List.class);
    List<Object> values =
        redis.execute(
            script,
            List.of(circuitKey(type)),
            UUID.randomUUID().toString(),
            Long.toString(properties.leaseMs()));
    boolean allowed = ((Number) values.get(0)).intValue() == 1;
    return new Permit(
        allowed,
        values.get(1).toString(),
        values.get(2).toString(),
        allowed ? 0 : Math.max(0.1, ((Number) values.get(3)).doubleValue() / 1000));
  }

  public boolean renewCircuit(String type, Permit permit) {
    if (permit.token().isEmpty()) return true;
    return Long.valueOf(1)
        .equals(
            redis.execute(
                script("circuit_renew"),
                List.of(circuitKey(type)),
                permit.epoch(),
                permit.token(),
                Long.toString(properties.leaseMs())));
  }

  public void finishCircuit(String type, Permit permit, boolean success) {
    if (!permit.token().isEmpty())
      redis.execute(
          script("circuit_finish"),
          List.of(circuitKey(type)),
          permit.epoch(),
          permit.token(),
          success ? "1" : "0",
          Long.toString((long) (properties.circuitCooldownSeconds() * 1000)));
  }

  public boolean heartbeat(String value) {
    String key = "worker:" + properties.id();
    String existing = redis.opsForValue().get(key);
    if (existing == null)
      return Boolean.TRUE.equals(
          redis
              .opsForValue()
              .setIfAbsent(key, value, Duration.ofSeconds(properties.heartbeatTtlSeconds())));
    return Long.valueOf(1)
        .equals(
            redis.execute(
                script("renew"),
                List.of(key),
                value,
                Integer.toString(properties.heartbeatTtlSeconds() * 1000)));
  }

  public void releaseHeartbeat(String value) {
    redis.execute(script("release"), List.of("worker:" + properties.id()), value);
  }
}

package io.taskscheduler.worker;

import jakarta.validation.constraints.*;
import java.util.UUID;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Bounded settings loaded through Spring configuration/environment, without embedded credentials.
 */
@ConfigurationProperties("worker")
@Validated
public record WorkerProperties(
    @Size(max = 255) String id,
    @NotBlank String groupId,
    @Min(1) @Max(16) int concurrency,
    @DecimalMin("0.1") double taskTimeoutSeconds,
    @DecimalMin("0.3") double visibilityTimeoutSeconds,
    @DecimalMin("0") double retryBaseSeconds,
    @DecimalMin("0") double retryCapSeconds,
    @Min(1) int maxCrashClaims,
    @Min(1) int heartbeatTtlSeconds,
    @NotBlank String circuitKeyPrefix,
    @DecimalMin("0.1") double circuitCooldownSeconds,
    @Min(1024) @Max(900000) int maxTaskMessageBytes,
    @Min(1) int publishTimeoutSeconds,
    @Min(1) int outboxBatchSize) {
  public WorkerProperties {
    if (id == null || id.isBlank()) id = "java-" + UUID.randomUUID();
    else if (!id.startsWith("java-")) id = "java-" + id;
    if (retryCapSeconds < retryBaseSeconds)
      throw new IllegalArgumentException("retry cap must cover base");
  }

  public long leaseMs() {
    return Math.max(1, (long) (visibilityTimeoutSeconds * 1000));
  }
}

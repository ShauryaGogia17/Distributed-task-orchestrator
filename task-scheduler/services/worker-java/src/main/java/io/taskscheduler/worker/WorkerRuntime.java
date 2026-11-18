package io.taskscheduler.worker;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Bounded handlers, renewable leases, deadline cancellation, and publication-before-ack. */
@Component
public class WorkerRuntime {
  private static final Logger log = LoggerFactory.getLogger(WorkerRuntime.class);
  private final WorkerStore store;
  private final RedisState redis;
  private final Handlers handlers;
  private final WorkerProperties properties;
  private final MeterRegistry metrics;
  private final ExecutorService pool;
  private final ScheduledExecutorService renewals;
  private final String heartbeat;
  private volatile boolean healthy = false;
  private final AtomicInteger inflight = new AtomicInteger();

  public WorkerRuntime(
      WorkerStore store,
      RedisState redis,
      Handlers handlers,
      WorkerProperties properties,
      MeterRegistry metrics) {
    this.store = store;
    this.redis = redis;
    this.handlers = handlers;
    this.properties = properties;
    this.metrics = metrics;
    metrics.gauge("worker_inflight", inflight);
    metrics.gauge("scheduler_is_leader", 0);
    metrics.gauge(
        "service_up", List.of(io.micrometer.core.instrument.Tag.of("service", "worker-java")), 1);
    pool = Executors.newFixedThreadPool(properties.concurrency());
    renewals = Executors.newScheduledThreadPool(properties.concurrency());
    heartbeat =
        Wire.encode(
            Map.of(
                "worker_id",
                properties.id(),
                "token",
                UUID.randomUUID().toString(),
                "language",
                "java"));
  }

  /** Called synchronously on the listener thread; Kafka maintains one ordered input at a time. */
  public boolean process(Wire.TaskMessage message) throws Exception {
    if (!message.type().startsWith("java.")) return true;
    String token = UUID.randomUUID().toString();
    if (!redis.acquire(message.taskId(), message.generation(), token)) return false;
    try {
      WorkerStore.Claim claim = store.claim(message, token);
      if (!claim.ready()) return false;
      if (claim.execution() == null) return waitPublished(claim.events());
      WorkerStore.Execution execution = claim.execution();
      MDC.put("task_id", execution.taskId().toString());
      MDC.put("trace_id", execution.traceId().toString().replace("-", ""));
      MDC.put("worker_id", properties.id());
      log.info("Attempt started");
      Map<String, String> context = MDC.getCopyOfContextMap();
      AtomicBoolean lost = new AtomicBoolean(false);
      Future<JsonNode> handler =
          pool.submit(
              () -> {
                if (context != null) MDC.setContextMap(context);
                inflight.incrementAndGet();
                try {
                  return handlers.execute(
                      execution.type(), execution.payload(), execution.attemptNo());
                } finally {
                  inflight.decrementAndGet();
                  MDC.clear();
                }
              });
      long period = Math.max(10, properties.leaseMs() / 3);
      ScheduledFuture<?> renewal =
          renewals.scheduleAtFixedRate(
              () -> {
                try {
                  if (!redis.renew(execution.taskId(), execution.generation(), execution.token())
                      || !redis.renewCircuit(execution.type(), execution.permit()))
                    throw new WorkerStore.LeaseLost();
                } catch (Exception e) {
                  lost.set(true);
                  handler.cancel(true);
                }
              },
              period,
              period,
              TimeUnit.MILLISECONDS);
      JsonNode result = null;
      Exception failure = null;
      try {
        try {
          result =
              handler.get(
                  Math.max(1, (long) (properties.taskTimeoutSeconds() * 1000)),
                  TimeUnit.MILLISECONDS);
          if (Wire.encode(result).getBytes(StandardCharsets.UTF_8).length
              > properties.maxTaskMessageBytes() - 2048)
            throw new Handlers.Fatal("Result exceeds Kafka envelope limit");
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof Error error) throw error;
          failure =
              cause instanceof Exception exception
                  ? exception
                  : new IllegalStateException("Handler failed");
        } catch (TimeoutException e) {
          handler.cancel(true);
          failure = new TimeoutException("Handler deadline exceeded");
        } catch (CancellationException e) {
          throw new WorkerStore.LeaseLost();
        } catch (Handlers.Fatal e) {
          failure = e;
        }
        if (lost.get()) throw new WorkerStore.LeaseLost();
        List<UUID> events = store.finish(execution, result, failure);
        redis.finishCircuit(execution.type(), execution.permit(), failure == null);
        if (failure == null) redis.completed(execution.taskId(), execution.generation());
        metrics
            .counter(
                "java_worker_attempts",
                "type",
                execution.type(),
                "outcome",
                failure == null ? "succeeded" : "failed")
            .increment();
        log.info("Attempt finished; outcome events committed");
        return waitPublished(events);
      } finally {
        handler.cancel(true);
        renewal.cancel(false);
      }
    } finally {
      redis.release(message.taskId(), message.generation(), token);
      MDC.clear();
    }
  }

  private boolean waitPublished(List<UUID> ids) throws InterruptedException {
    long deadline =
        System.nanoTime() + TimeUnit.SECONDS.toNanos(properties.publishTimeoutSeconds() + 5L);
    do {
      if (store.published(ids)) return true;
      Thread.sleep(50);
    } while (System.nanoTime() < deadline);
    return false;
  }

  @Scheduled(fixedDelay = 5000, initialDelay = 0)
  public void heartbeat() {
    try {
      healthy = redis.heartbeat(heartbeat);
    } catch (Exception e) {
      healthy = false;
      log.warn("Heartbeat unavailable: {}", e.getClass().getSimpleName());
    }
  }

  public boolean healthy() {
    return healthy;
  }

  @Scheduled(fixedDelay = 250, initialDelay = 1000)
  public void retryMaintenance() {
    try {
      store.recoverExpired();
      store.dispatchRetries();
    } catch (Exception e) {
      log.warn("Retry transaction retained: {}", e.getClass().getSimpleName());
    }
  }

  @PreDestroy
  public void close() throws InterruptedException {
    pool.shutdown();
    if (!pool.awaitTermination(90, TimeUnit.SECONDS)) pool.shutdownNow();
    renewals.shutdownNow();
    try {
      redis.releaseHeartbeat(heartbeat);
    } catch (Exception e) {
      log.warn("Heartbeat expires by TTL");
    }
  }
}

package io.taskscheduler.worker;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.*;
import jakarta.persistence.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** The same read-only durable snapshot as Python; dashboards deduplicate replicas with max. */
@Component
public class DurableMetrics implements MultiCollector {
  @PersistenceContext private EntityManager em;
  private final TransactionTemplate tx;
  private final StringRedisTemplate redis;
  private final WorkerProperties properties;
  private final int stuckSeconds;
  private final String sql;
  private static final double[] BOUNDS = {
    .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, Double.POSITIVE_INFINITY
  };

  public DurableMetrics(
      PlatformTransactionManager manager,
      StringRedisTemplate redis,
      WorkerProperties properties,
      PrometheusMeterRegistry registry,
      @Value("${METRICS_STUCK_RUNNING_SECONDS:300}") int stuckSeconds)
      throws Exception {
    this.tx = new TransactionTemplate(manager);
    tx.setReadOnly(true);
    tx.setTimeout(5);
    this.redis = redis;
    this.properties = properties;
    this.stuckSeconds = stuckSeconds;
    this.sql =
        new ClassPathResource("observability.sql").getContentAsString(StandardCharsets.UTF_8);
    registry.getPrometheusRegistry().register(this);
  }

  private static CounterSnapshot counter(String name, String help, double value) {
    return CounterSnapshot.builder()
        .name(name)
        .help(help)
        .dataPoint(CounterSnapshot.CounterDataPointSnapshot.builder().value(value).build())
        .build();
  }

  private static GaugeSnapshot gauge(String name, String help, double value) {
    return GaugeSnapshot.builder()
        .name(name)
        .help(help)
        .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder().value(value).build())
        .build();
  }

  @Override
  public MetricSnapshots collect() {
    JsonNode data =
        tx.execute(
            status -> {
              em.createNativeQuery("set local statement_timeout='4500ms'").executeUpdate();
              Object result =
                  em.createNativeQuery(
                          "select snapshot::text from (" + sql + ") as metrics(snapshot)")
                      .setParameter("stuck_seconds", stuckSeconds)
                      .getSingleResult();
              try {
                return Wire.JSON.readTree(result.toString());
              } catch (Exception e) {
                throw new IllegalStateException("Invalid metric snapshot", e);
              }
            });
    var metrics = MetricSnapshots.builder();
    metrics.metricSnapshot(
        counter(
            "tasks_submitted",
            "Retained committed task submissions",
            data.path("submitted").asDouble()));
    var processed =
        CounterSnapshot.builder().name("tasks_processed").help("Durable attempt outcomes");
    var retries = CounterSnapshot.builder().name("retries").help("Durable retry decisions");
    for (JsonNode row : data.path("processed")) {
      processed.dataPoint(
          CounterSnapshot.CounterDataPointSnapshot.builder()
              .labels(
                  Labels.of(
                      "status", row.path("status").asText(), "type", row.path("type").asText()))
              .value(row.path("value").asDouble())
              .build());
      if (row.path("status").asText().equals("RETRYING"))
        retries.dataPoint(
            CounterSnapshot.CounterDataPointSnapshot.builder()
                .labels(Labels.of("type", row.path("type").asText()))
                .value(row.path("value").asDouble())
                .build());
    }
    metrics.metricSnapshot(processed.build()).metricSnapshot(retries.build());
    var latency =
        HistogramSnapshot.builder()
            .name("task_latency_seconds")
            .help("Attempt execution time excluding scheduled wait");
    for (JsonNode row : data.path("latency")) {
      long[] counts = new long[BOUNDS.length];
      long previous = 0;
      for (int i = 0; i < BOUNDS.length; i++) {
        String key =
            Double.isInfinite(BOUNDS[i])
                ? "+Inf"
                : java.math.BigDecimal.valueOf(BOUNDS[i]).stripTrailingZeros().toPlainString();
        long cumulative = row.path("buckets").path(key).asLong();
        counts[i] = cumulative - previous;
        previous = cumulative;
      }
      latency.dataPoint(
          HistogramSnapshot.HistogramDataPointSnapshot.builder()
              .labels(Labels.of("type", row.path("type").asText()))
              .sum(row.path("sum").asDouble())
              .classicHistogramBuckets(ClassicHistogramBuckets.of(BOUNDS, counts))
              .build());
    }
    metrics.metricSnapshot(latency.build());
    var dlq = GaugeSnapshot.builder().name("dlq_size").help("OPEN DLQ incidents by task type");
    for (JsonNode row : data.path("dlq"))
      dlq.dataPoint(
          GaugeSnapshot.GaugeDataPointSnapshot.builder()
              .labels(Labels.of("type", row.path("type").asText()))
              .value(row.path("value").asDouble())
              .build());
    metrics.metricSnapshot(dlq.build());
    for (String[] item :
        new String[][] {
          {"outbox_lag", "outbox_lag"},
          {"outbox_pending", "outbox_pending"},
          {"running_tasks", "running"},
          {"running_tasks_oldest_age_seconds", "running_age"},
          {"stuck_running_tasks", "stuck_running"}
        })
      metrics.metricSnapshot(gauge(item[0], "Durable task state", data.path(item[1]).asDouble()));
    var diagnoses =
        CounterSnapshot.builder()
            .name("agent_diagnoses")
            .help("Committed per-incident agent decisions");
    for (JsonNode row : data.path("diagnoses"))
      diagnoses.dataPoint(
          CounterSnapshot.CounterDataPointSnapshot.builder()
              .labels(
                  Labels.of(
                      "category",
                      row.path("category").asText(),
                      "action",
                      row.path("action").asText()))
              .value(row.path("value").asDouble())
              .build());
    metrics.metricSnapshot(diagnoses.build());
    var tokens =
        CounterSnapshot.builder()
            .name("agent_tokens")
            .help("Known provider-reported tokens once per LLM turn");
    for (String direction : List.of("input", "output"))
      tokens.dataPoint(
          CounterSnapshot.CounterDataPointSnapshot.builder()
              .labels(Labels.of("direction", direction))
              .value(data.path("tokens").path(direction).asDouble())
              .build());
    metrics.metricSnapshot(tokens.build());
    metrics.metricSnapshot(
        counter(
            "agent_tokens_charged",
            "Conservatively budget-charged tokens",
            data.path("tokens").path("charged").asDouble()));
    metrics.metricSnapshot(
        counter(
            "agent_cost_usd",
            "Budget-accounted estimated USD",
            data.path("tokens").path("cost").asDouble()));
    var circuits =
        GaugeSnapshot.builder()
            .name("circuit_breaker_open")
            .help("Circuit is not CLOSED including half-open probes");
    List<String> types = new ArrayList<>();
    List<byte[]> keys = new ArrayList<>();
    for (JsonNode type : data.path("types")) {
      types.add(type.asText());
      try {
        keys.add(
            (properties.circuitKeyPrefix()
                    + HexFormat.of()
                        .formatHex(
                            MessageDigest.getInstance("SHA-256")
                                .digest(type.asText().getBytes(StandardCharsets.UTF_8))))
                .getBytes(StandardCharsets.UTF_8));
      } catch (java.security.NoSuchAlgorithmException e) {
        throw new IllegalStateException(e);
      }
    }
    List<Object> states =
        keys.isEmpty()
            ? List.of()
            : redis.executePipelined(
                (org.springframework.data.redis.core.RedisCallback<Object>)
                    connection -> {
                      for (byte[] key : keys) connection.keyCommands().exists(key);
                      return null;
                    });
    for (int i = 0; i < types.size(); i++) {
      Object state = states.get(i);
      boolean opened =
          Boolean.TRUE.equals(state) || (state instanceof Number n && n.longValue() > 0);
      circuits.dataPoint(
          GaugeSnapshot.GaugeDataPointSnapshot.builder()
              .labels(Labels.of("type", types.get(i)))
              .value(opened ? 1 : 0)
              .build());
    }
    return metrics.metricSnapshot(circuits.build()).build();
  }
}

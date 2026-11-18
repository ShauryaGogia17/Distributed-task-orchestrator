package io.taskscheduler.worker;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;

/** Restore thread-local task correlation after listener and publisher scopes. */
public final class Correlation implements AutoCloseable {
  private final Map<String, String> previous = MDC.getCopyOfContextMap();

  private Correlation(String task, String trace) {
    MDC.put("task_id", task);
    String normalized;
    try {
      String raw = trace.replace("-", "");
      if (!raw.matches("[0-9a-fA-F]{32}") || raw.matches("0{32}"))
        throw new IllegalArgumentException();
      normalized = raw.toLowerCase(java.util.Locale.ROOT);
    } catch (Exception e) {
      normalized = UUID.randomUUID().toString().replace("-", "");
    }
    MDC.put("trace_id", normalized);
  }

  public static Correlation bind(String task, String trace) {
    return new Correlation(task, trace);
  }

  public static Correlation bind(ConsumerRecord<?, ?> record, String task) {
    var header = record.headers().lastHeader("trace_id");
    return bind(task, header == null ? "" : new String(header.value(), StandardCharsets.US_ASCII));
  }

  @Override
  public void close() {
    MDC.clear();
    if (previous != null) MDC.setContextMap(previous);
  }
}

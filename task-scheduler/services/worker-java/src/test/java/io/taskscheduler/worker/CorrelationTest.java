package io.taskscheduler.worker;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class CorrelationTest {
  @Test
  void kafkaTraceNormalizesAndRestoresCallerContext() {
    MDC.put("trace_id", "outer");
    String trace = UUID.randomUUID().toString();
    var record = new ConsumerRecord<String, String>("tasks.pending", 0, 0, "key", "{}");
    record.headers().add("trace_id", trace.getBytes(StandardCharsets.US_ASCII));
    try (Correlation ignored = Correlation.bind(record, "task-1")) {
      assertEquals(trace.replace("-", ""), MDC.get("trace_id"));
      assertEquals("task-1", MDC.get("task_id"));
    }
    assertEquals("outer", MDC.get("trace_id"));
    assertNull(MDC.get("task_id"));
    MDC.clear();
  }

  @Test
  void missingOrInvalidTraceStartsNonzeroTrace() {
    for (String invalid : new String[] {"", "nonsense", "0".repeat(32)}) {
      try (Correlation ignored = Correlation.bind("task-1", invalid)) {
        assertTrue(MDC.get("trace_id").matches("[0-9a-f]{32}"));
        assertNotEquals("0".repeat(32), MDC.get("trace_id"));
      }
    }
  }
}

package io.taskscheduler.worker;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

/** Python-generated fixtures reach the real Java listener, and Java emits a reverse task. */
class ContractTest {
  private String resource(String name) throws Exception {
    try (var input = getClass().getResourceAsStream("/contracts/" + name)) {
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  @Test
  void pythonTaskIsConsumedByJavaAndJavaTaskIsWrittenForPython() throws Exception {
    String serialized = resource("python-task.json");
    WorkerRuntime runtime = mock(WorkerRuntime.class);
    WorkerStore store = mock(WorkerStore.class);
    when(runtime.process(any())).thenReturn(true);
    Acknowledgment ack = mock(Acknowledgment.class);
    new KafkaListeners(runtime, store)
        .pending(new ConsumerRecord<>("tasks.pending", 0, 7, "high:test", serialized), ack);
    var capture = org.mockito.ArgumentCaptor.forClass(Wire.TaskMessage.class);
    verify(runtime).process(capture.capture());
    verify(ack).acknowledge();
    Wire.TaskMessage task = capture.getValue();
    assertEquals("java.data_transform", task.type());
    assertEquals(2, task.generation());
    assertEquals("東京", task.payload().path("values").get(2).asText());
    assertEquals(
        9007199254740993L, task.payload().path("metadata").path("large_integer").longValue());
    assertEquals(123456000, task.scheduledAt().getNano());
    assertTrue(task.aiModified());
    ObjectNode payload = task.payload().deepCopy();
    payload.put("origin", "java");
    Wire.TaskMessage reverse =
        new Wire.TaskMessage(
            1,
            false,
            0,
            UUID.fromString("b8a1195f-2065-4a49-936a-20d22275aad6"),
            "generate_report",
            payload,
            5,
            1,
            3,
            OffsetDateTime.parse("2026-11-10T12:30:00.123456Z"),
            OffsetDateTime.parse("2026-11-10T12:00:00Z"));
    Path output = Path.of("target/contracts/java-task.json");
    Files.createDirectories(output.getParent());
    Files.writeString(output, Wire.encode(reverse), StandardCharsets.UTF_8);
    Path roundTrip = Path.of("target/contracts/python-roundtrip.json");
    Files.writeString(roundTrip, Wire.encode(task), StandardCharsets.UTF_8);
  }

  @Test
  void identitiesPartitionsAndFingerprintsMatchPython() throws Exception {
    JsonNode vectors = Wire.JSON.readTree(resource("python-vectors.json"));
    UUID id = UUID.fromString(vectors.path("task_id").asText());
    for (JsonNode vector : vectors.path("event_ids"))
      assertEquals(
          vector.path("id").asText(),
          Wire.eventId(
                  id,
                  vector.path("attempt").asInt(),
                  vector.path("kind").asText(),
                  vector.path("generation").asInt())
              .toString());
    for (JsonNode vector : vectors.path("partitions")) {
      assertEquals(vector.path("key").asText(), Wire.key(id, vector.path("priority").asInt()));
      assertEquals(vector.path("partition").asInt(), Wire.partition(vector.path("key").asText()));
    }
    for (JsonNode vector : vectors.path("fingerprints")) {
      assertEquals(
          vector.path("normalized").asText(), Fingerprint.normalize(vector.path("error").asText()));
      assertEquals(
          vector.path("signature").asText(),
          Fingerprint.signature(vector.path("error").asText(), vector.path("trace").asText()));
    }
  }

  @Test
  void missingOptionalFieldsUsePythonDefaultsAndUnknownFieldsAreRejected() {
    String minimal =
        "{\"task_id\":\"77342418-78a5-4bdf-8591-ab0467e5a9f4\",\"type\":\"java.data_transform\",\"scheduled_at\":\"2026-11-10T12:00:00Z\"}";
    Wire.TaskMessage task = Wire.task(minimal);
    assertEquals(1, task.attemptNo());
    assertEquals(0, task.generation());
    assertEquals(3, task.maxRetries());
    assertThrows(
        IllegalArgumentException.class,
        () -> Wire.task(minimal.replace("}", ",\"unknown\":true}")));
    assertThrows(
        IllegalArgumentException.class,
        () -> Wire.task(minimal.replace("}", ",\"priority\":1.5}")));
    assertThrows(
        IllegalArgumentException.class,
        () -> Wire.task(minimal.replace("}", ",\"generation\":null}")));
  }
}

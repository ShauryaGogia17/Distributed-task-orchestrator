package io.taskscheduler.worker;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.pdfbox.Loader;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

class HandlersTest {
  private final Handlers handlers = new Handlers();

  @Test
  void pdfContainsRealReadablePages() throws Exception {
    var result =
        handlers.execute(
            "java.pdf_render",
            Wire.JSON.readTree("{\"text\":\"Hello distributed workers\",\"pages\":2}"),
            1);
    byte[] bytes = Base64.getDecoder().decode(result.path("pdf_base64").asText());
    try (var pdf = Loader.loadPDF(bytes)) {
      assertEquals(2, pdf.getNumberOfPages());
    }
    assertEquals(bytes.length, result.path("size_bytes").asInt());
  }

  @Test
  void transformationsAndRetryClassificationAreDeterministic() throws Exception {
    var input =
        Wire.JSON.readTree(
            "{\"operation\":\"scale\",\"values\":[1,2.5],\"factor\":3,\"transient_failures\":1}");
    assertThrows(Handlers.Retryable.class, () -> handlers.execute("java.data_transform", input, 1));
    var result = handlers.execute("java.data_transform", input, 2);
    assertEquals(7.5, result.path("values").get(1).doubleValue());
    assertThrows(
        Handlers.Fatal.class, () -> handlers.execute("java.data_transform", Wire.object(), 1));
    assertEquals("tasks.retry.5s", Wire.retryTopic(1));
    assertEquals("tasks.retry.30s", Wire.retryTopic(2));
    assertEquals("tasks.retry.5m", Wire.retryTopic(3));
  }

  @Test
  void pythonTypesAreAcknowledgedWithoutCallingJavaRuntime() throws Exception {
    WorkerRuntime runtime = mock(WorkerRuntime.class);
    WorkerStore store = mock(WorkerStore.class);
    Acknowledgment ack = mock(Acknowledgment.class);
    String json = "{\"type\":\"generate_report\",\"future_python_field\":true}";
    new KafkaListeners(runtime, store)
        .pending(new ConsumerRecord<>("tasks.pending", 0, 0, "k", json), ack);
    verifyNoInteractions(runtime);
    verify(ack).acknowledge();
  }

  @Test
  void incompletePublicationIsNotAcknowledged() throws Exception {
    WorkerRuntime runtime = mock(WorkerRuntime.class);
    WorkerStore store = mock(WorkerStore.class);
    Acknowledgment ack = mock(Acknowledgment.class);
    String json =
        "{\"task_id\":\"77342418-78a5-4bdf-8591-ab0467e5a9f4\",\"type\":\"java.data_transform\",\"scheduled_at\":\"2026-11-10T12:00:00Z\"}";
    when(runtime.process(any())).thenReturn(false);
    new KafkaListeners(runtime, store)
        .pending(new ConsumerRecord<>("tasks.pending", 0, 0, "k", json), ack);
    verify(ack, never()).acknowledge();
    verify(ack).nack(java.time.Duration.ofSeconds(1));
  }
}

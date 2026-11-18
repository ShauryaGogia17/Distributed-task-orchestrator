package io.taskscheduler.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.util.*;
import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.pdmodel.font.*;
import org.springframework.stereotype.Component;

/** Bounded runnable handlers; PDF results contain real PDF bytes, not a simulation. */
@Component
public class Handlers {
  public static class Fatal extends RuntimeException {
    public Fatal(String message) {
      super("ValidationError: " + message);
    }
  }

  public static class Retryable extends RuntimeException {
    public Retryable(String message) {
      super(message);
    }
  }

  public JsonNode execute(String type, JsonNode payload, int attempt) throws Exception {
    int failures = integer(payload, "transient_failures", 0, 0, 100);
    if (attempt <= failures) throw new Retryable("Temporary data source unavailable");
    int sleep = integer(payload, "sleep_ms", 0, 0, 60000);
    if (sleep > 0) Thread.sleep(sleep);
    return switch (type) {
      case "java.pdf_render" -> pdf(payload);
      case "java.data_transform" -> transform(payload);
      default -> throw new Fatal("Unknown Java task type");
    };
  }

  private int integer(JsonNode payload, String key, int fallback, int min, int max) {
    if (!payload.has(key)) return fallback;
    JsonNode value = payload.get(key);
    if (!value.isIntegralNumber()
        || !value.canConvertToInt()
        || value.intValue() < min
        || value.intValue() > max) throw new Fatal("Invalid " + key);
    return value.intValue();
  }

  private JsonNode transform(JsonNode payload) {
    JsonNode values = payload.get("values");
    if (values == null || !values.isArray() || values.size() > 10000)
      throw new Fatal("values must be an array of at most 10000 items");
    String operation = payload.path("operation").asText("uppercase");
    ArrayNode output = Wire.JSON.createArrayNode();
    if (operation.equals("uppercase")) {
      for (JsonNode value : values) {
        if (!value.isTextual()) throw new Fatal("uppercase requires string values");
        output.add(value.textValue().toUpperCase(Locale.ROOT));
      }
    } else if (operation.equals("scale")) {
      JsonNode factor = payload.get("factor");
      if (factor == null || !factor.isNumber()) throw new Fatal("scale requires a numeric factor");
      BigDecimal multiplier = factor.decimalValue();
      for (JsonNode value : values) {
        if (!value.isNumber()) throw new Fatal("scale requires numeric values");
        output.add(value.decimalValue().multiply(multiplier));
      }
    } else throw new Fatal("operation must be uppercase or scale");
    ObjectNode result = Wire.object();
    result.set("values", output);
    return result.put("count", output.size());
  }

  private JsonNode pdf(JsonNode payload) throws Exception {
    if (!payload.path("text").isTextual()) throw new Fatal("text is required");
    String text = payload.path("text").textValue();
    if (text.length() > 20000) throw new Fatal("text exceeds 20000 characters");
    int pages = integer(payload, "pages", 1, 1, 10);
    byte[] bytes;
    try (PDDocument document = new PDDocument();
        ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      PDType1Font font = new PDType1Font(Standard14Fonts.FontName.HELVETICA);
      for (int pageNumber = 0; pageNumber < pages; pageNumber++) {
        PDPage page = new PDPage();
        document.addPage(page);
        try (PDPageContentStream content = new PDPageContentStream(document, page)) {
          content.beginText();
          content.setFont(font, 11);
          content.setLeading(14);
          content.newLineAtOffset(40, 750);
          int lines = 0;
          for (String line : text.replace("\r", "").split("\n", -1)) {
            if (++lines > 48) throw new Fatal("A page supports at most 48 lines");
            if (line.length() > 100) throw new Fatal("A line supports at most 100 characters");
            try {
              content.showText(line);
            } catch (IllegalArgumentException e) {
              throw new Fatal("PDF text requires Helvetica/WinAnsi-compatible characters");
            }
            content.newLine();
          }
          content.endText();
        }
      }
      document.save(output);
      bytes = output.toByteArray();
    }
    return Wire.object()
        .put("content_type", "application/pdf")
        .put("pages", pages)
        .put("size_bytes", bytes.length)
        .put("sha256", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)))
        .put("pdf_base64", Base64.getEncoder().encodeToString(bytes));
  }
}

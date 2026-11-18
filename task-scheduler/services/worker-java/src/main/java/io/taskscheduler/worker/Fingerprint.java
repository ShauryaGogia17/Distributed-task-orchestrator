package io.taskscheduler.worker;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Byte-compatible normalization v1 used by shared/fingerprint.py and the DLQ consumer. */
public final class Fingerprint {
  private static Pattern pattern(String regex) {
    return Pattern.compile(regex, Pattern.UNICODE_CHARACTER_CLASS);
  }

  private static final Pattern ID =
      pattern(
          "(?i)[\"']?\\b((?:(?:task|worker|request|trace|span|user|job|correlation)_?)?id)[\"']?\\s*[:=]\\s*[\"']?[a-z0-9_.:\\-]+[\"']?");
  private static final Pattern TIME =
      pattern("\\b\\d{4}-\\d{2}-\\d{2}[T ][0-9:.]+(?:Z|[+-]\\d{2}:?\\d{2})?\\b");
  private static final Pattern UUID =
      pattern("(?i)\\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\b");
  private static final Pattern HEX = pattern("(?i)\\b(?:0x[0-9a-f]+|[0-9a-f]{16,})\\b");
  private static final Pattern FILE = pattern("File \"([^\"]+)\"");
  private static final Pattern NUMBER = pattern("\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?");
  private static final Pattern SPACE = pattern("\\s+");

  private Fingerprint() {}

  public static String normalize(String text) {
    String value = ID.matcher(text).replaceAll(m -> m.group(1).toLowerCase(Locale.ROOT) + "=<id>");
    value = TIME.matcher(value).replaceAll("<timestamp>");
    value = UUID.matcher(value).replaceAll("<id>");
    value = HEX.matcher(value).replaceAll("<id>");
    value =
        FILE.matcher(value)
            .replaceAll(
                m -> {
                  String path = m.group(1).replace('\\', '/');
                  return Matcher.quoteReplacement(
                      "File \"<path>/" + path.substring(path.lastIndexOf('/') + 1) + "\"");
                });
    value = NUMBER.matcher(value).replaceAll("<number>");
    return SPACE.matcher(value.toLowerCase(Locale.ROOT)).replaceAll(" ").strip();
  }

  public static String signature(String error, String trace) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(
                      ("v1\n" + normalize(error) + "\n" + normalize(trace == null ? "" : trace))
                          .getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}

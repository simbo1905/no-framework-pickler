// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CustomTypesTest {
  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  // Define the record that uses the custom types
  public record RecordWithCustomTypes(
          UUID eventId,
          String eventName,
          SimpleTimeZone eventTimeZone
  ) {}

  // Define the SerdeHandlers (as shown in Step 1 and 2)
  public static final SerdeHandler UUID_HANDLER = new SerdeHandler(
          UUID.class,
          1, // A unique positive marker for this custom type
          (obj) -> 2 * Long.BYTES, // Sizer
          (buffer, obj) -> { // Writer
            UUID uuid = (UUID) obj;
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
          },
          (buffer) -> { // Reader
            long mostSigBits = buffer.getLong();
            long leastSigBits = buffer.getLong();
            return new UUID(mostSigBits, leastSigBits);
          }
  );

  public static final SerdeHandler SIMPLE_TIME_ZONE_HANDLER = new SerdeHandler(
          SimpleTimeZone.class,
          2, // A unique positive marker
          (obj) -> { // Sizer
            SimpleTimeZone tz = (SimpleTimeZone) obj;
            byte[] idBytes = tz.getID().getBytes(StandardCharsets.UTF_8);
            // Size of raw offset + size of ID length + size of ID bytes
            return Integer.BYTES + Integer.BYTES + idBytes.length;
          },
          (buffer, obj) -> { // Writer
            SimpleTimeZone tz = (SimpleTimeZone) obj;
            buffer.putInt(tz.getRawOffset());
            byte[] idBytes = tz.getID().getBytes(StandardCharsets.UTF_8);
            buffer.putInt(idBytes.length);
            buffer.put(idBytes);
          },
          (buffer) -> { // Reader
            int rawOffset = buffer.getInt();
            int idLength = buffer.getInt();
            byte[] idBytes = new byte[idLength];
            buffer.get(idBytes);
            String id = new String(idBytes, StandardCharsets.UTF_8);
            return new SimpleTimeZone(rawOffset, id);
          }
  );

  @Test
  void testCustomTypeHandlers() {
    // 1. Prepare test data
    final var originalRecord = new RecordWithCustomTypes(
            UUID.randomUUID(),
            "Framework Demo",
            new SimpleTimeZone(3600000, "Europe/London")
    );

    // 2. Create the pickler with custom handlers
    final var pickler = Pickler.forClass(
            RecordWithCustomTypes.class,
            Map.of(),
            List.of(UUID_HANDLER, SIMPLE_TIME_ZONE_HANDLER)
    );

    // 3. Allocate a buffer and perform serialization
    final ByteBuffer buffer = ByteBuffer.allocate(pickler.maxSizeOf(originalRecord));
    pickler.serialize(buffer, originalRecord);
    buffer.flip(); // Prepare buffer for reading

    // 4. Perform deserialization
    final var deserializedRecord = pickler.deserialize(buffer);

    // 5. Assert that the original and deserialized records are equal
    assertEquals(originalRecord, deserializedRecord);
    assertEquals(originalRecord.eventId(), deserializedRecord.eventId());
    assertEquals(originalRecord.eventTimeZone().getRawOffset(), deserializedRecord.eventTimeZone().getRawOffset());
    assertEquals(originalRecord.eventTimeZone().getID(), deserializedRecord.eventTimeZone().getID());
  }
}

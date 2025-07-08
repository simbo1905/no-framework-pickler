package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/// Test class for UUID support in the Pickler framework.
/// This test verifies that UUIDs can be properly serialized and deserialized
/// using the framework's public API.
public class UuidSupportTests {
  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  /// Public record containing a UUID field for testing serialization.
  /// This record must be public as required by the Pickler framework.
  public record UserSession(String sessionId, UUID userId, long timestamp) {
  }

  @Test
  void testUuidRoundTripSerialization() {
    LOGGER.info("Starting UUID round-trip serialization test");

    // Create a UUID from known values for predictable testing
    final long mostSigBits = 0x550e8400e29b41d4L;
    final long leastSigBits = 0xa716446655440000L;
    final var originalUuid = new UUID(mostSigBits, leastSigBits);

    LOGGER.info(() -> "Created test UUID: " + originalUuid);
    LOGGER.info(() -> "UUID mostSigBits: " + Long.toHexString(mostSigBits));
    LOGGER.info(() -> "UUID leastSigBits: " + Long.toHexString(leastSigBits));

    // Create a record containing the UUID
    final var originalRecord = new UserSession("session-123", originalUuid, System.currentTimeMillis());
    LOGGER.info(() -> "Created test record: " + originalRecord);

    // Get a pickler for the record type
    final var pickler = Pickler.forClass(UserSession.class);
    assertNotNull(pickler, "Pickler should not be null");
    LOGGER.info("Successfully created pickler for UserSession record");

    // Calculate size and allocate buffer
    final int size = pickler.maxSizeOf(originalRecord);
    LOGGER.info(() -> "Calculated serialized size: " + size + " bytes");

    final var buffer = ByteBuffer.allocate(size);
    LOGGER.info(() -> "Allocated buffer with capacity: " + buffer.capacity());

    // Serialize the record
    pickler.serialize(buffer, originalRecord);
    LOGGER.info(() -> "Serialized record, buffer position: " + buffer.position());

    // Prepare buffer for reading
    buffer.flip();
    LOGGER.info(() -> "Buffer flipped for reading, limit: " + buffer.limit());

    // Deserialize the record
    final var deserializedRecord = pickler.deserialize(buffer);
    assertNotNull(deserializedRecord, "Deserialized record should not be null");
    LOGGER.info(() -> "Deserialized record: " + deserializedRecord);

    // Verify the entire record matches
    assertEquals(originalRecord, deserializedRecord, "Original and deserialized records should be equal");
    LOGGER.info("Record equality verification passed");

    // Verify UUID specifically
    assertEquals(originalRecord.userId(), deserializedRecord.userId(), "UUIDs should be equal");
    assertEquals(originalUuid, deserializedRecord.userId(), "Deserialized UUID should match original");

    // Verify UUID components match
    assertEquals(mostSigBits, deserializedRecord.userId().getMostSignificantBits(),
        "Most significant bits should match");
    assertEquals(leastSigBits, deserializedRecord.userId().getLeastSignificantBits(),
        "Least significant bits should match");

    LOGGER.info("UUID round-trip serialization test completed successfully");
  }
}

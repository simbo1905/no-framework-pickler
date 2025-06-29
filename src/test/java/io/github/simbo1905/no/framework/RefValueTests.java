// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.*;

import java.nio.ByteBuffer;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

/// Package-private tests for core machinery components
/// Tests the internal implementation details that are not part of the public API
public class RefValueTests {

  public static RefValueRecord pefValueRecordNotNull =
      new RefValueRecord(true, (byte) 1, 'a', (short) 2, 3, 4L, 5.0f, 6.0);

  public static RefValueRecord refValueRecordNull =
      new RefValueRecord(null, null, null, null, null, null, null, null);

  @BeforeAll
  static void setupLogging() {
    io.github.simbo1905.LoggingControl.setupCleanLogging();
  }

  @BeforeEach
  void setUp() {
    LOGGER.fine(() -> "Starting RefValueTests test");
  }

  @AfterEach
  void tearDown() {
    LOGGER.fine(() -> "Finished RefValueTests test");
  }

  @Test
  @DisplayName("Test references not null")
  void testReferencesNotNull() {
    final var pickler = Pickler.forClass(RefValueRecord.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(pefValueRecordNotNull));
    pickler.serialize(buffer, pefValueRecordNotNull);
    buffer.flip(); // Prepare for reading
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(pefValueRecordNotNull);

  }

  @Test
  @DisplayName("Test references are null")
  void testReferencesAreull() {
    final var pickler = Pickler.forClass(RefValueRecord.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(refValueRecordNull));
    pickler.serialize(buffer, refValueRecordNull);
    buffer.flip(); // Prepare for reading
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(refValueRecordNull);

  }

  public record RefValueRecord(
      Boolean booleanValue,
      Byte byteValue,
      Character charValue,
      Short shortValue,
      Integer intValue,
      Long longValue,
      Float floatValue,
      Double doubleValue
  ) {
  }
}

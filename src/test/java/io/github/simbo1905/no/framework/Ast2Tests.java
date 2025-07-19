// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class Ast2Tests {

  private static final Logger LOGGER = Logger.getLogger(Ast2Tests.class.getName());

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  // A trivial record for testing
  public record TrivialRecord(int value) {
  }

  @Test
  void testTrivialRecordRoundTrip() {
    // 1. Get a pickler for the trivial record
    final Pickler<TrivialRecord> pickler = Pickler.forClass(TrivialRecord.class);

    // 2. Create an instance of the record
    final var original = new TrivialRecord(42);
    LOGGER.fine(() -> "Original record: " + original);
    LOGGER.fine(() -> "Original record hashCode: " + original.hashCode());


    // 3. Use maxSizeOf to create a ByteBuffer
    final int maxSize = pickler.maxSizeOf(original);
    LOGGER.fine(() -> "maxSizeOf: " + maxSize);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSize);

    // 4. Write the record to the buffer
    final int bytesWritten = pickler.serialize(buffer, original);
    LOGGER.fine(() -> "Bytes written: " + bytesWritten);


    // 5. Flip the buffer for reading
    buffer.flip();

    // 6. Read the record back
    final TrivialRecord deserialized = pickler.deserialize(buffer);
    LOGGER.fine(() -> "Deserialized record: " + deserialized);

    // 7. Assert the deserialized record is equal to the original
    assertEquals(original, deserialized);
  }

  @Test
  void testLinkedListRecordRoundTrip() {
    // 1. Get a pickler for the linked list record
    final Pickler<LinkedListNode> pickler = Pickler.forClass(LinkedListNode.class);

    // 2. Create a sample linked list
    final var originalList = new LinkedListNode(3, new LinkedListNode(2, new LinkedListNode(1)));

    LOGGER.fine(() -> String.format(
        "Original linked list: %s (hashCode: %d)",
        originalList,
        originalList.hashCode()
    ));

    // 3. Calculate max size and allocate buffer
    final int maxSize = pickler.maxSizeOf(originalList);
    LOGGER.fine(() -> "Max size: " + maxSize);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSize);

    // 4. Serialize the linked list
    final int bytesWritten = pickler.serialize(buffer, originalList);
    LOGGER.fine(() -> "Bytes written: " + bytesWritten);

    // 5. Flip buffer for reading
    buffer.flip();

    // 6. Deserialize the linked list
    final LinkedListNode deserializedList = pickler.deserialize(buffer);
    LOGGER.fine(() -> "Deserialized linked list: " + deserializedList);

    // 7. Assert equality
    assertEquals(originalList, deserializedList,
        "Deserialized linked list should equal the original linked list");
  }

  // Add the nested record definition
  public record LinkedListNode(int value, LinkedListNode next) {
    public LinkedListNode(int value) {
      this(value, null);
    }
  }

  // For nested record test
  public record NestedRecord(String name) {
  }

  public record OuterRecord(int id, NestedRecord nested) {
  }

  // For enum test
  public enum Color {RED, GREEN, BLUE}

  public record RecordWithEnum(String name, Color color) {
  }

  // For empty record test
  public record EmptyRecord() {
  }

  public record RecordWithEmpty(int id, EmptyRecord empty) {
  }

  @Test
  void testNestedRecordRoundTrip() {
    // 1. Get a pickler for the trivial record
    final Pickler<OuterRecord> pickler = Pickler.forClass(OuterRecord.class);

    // 2. Create an instance of the record
    final var original = new OuterRecord(1, new NestedRecord("test"));
    LOGGER.fine(() -> "Original record: " + original);
    LOGGER.fine(() -> "Original record hashCode: " + original.hashCode());


    // 3. Use maxSizeOf to create a ByteBuffer
    final int maxSize = pickler.maxSizeOf(original);
    LOGGER.fine(() -> "maxSizeOf: " + maxSize);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSize);

    // 4. Write the record to the buffer
    final int bytesWritten = pickler.serialize(buffer, original);
    LOGGER.fine(() -> "Bytes written: " + bytesWritten);


    // 5. Flip the buffer for reading
    buffer.flip();

    // 6. Read the record back
    final OuterRecord deserialized = pickler.deserialize(buffer);
    LOGGER.fine(() -> "Deserialized record: " + deserialized);

    // 7. Assert the deserialized record is equal to the original
    assertEquals(original, deserialized);
  }

  @Test
  void testRecordWithEnumRoundTrip() {
    // 1. Get a pickler for the trivial record
    final Pickler<RecordWithEnum> pickler = Pickler.forClass(RecordWithEnum.class);

    // 2. Create an instance of the record
    final var original = new RecordWithEnum("car", Color.BLUE);
    LOGGER.fine(() -> "Original record: " + original);
    LOGGER.fine(() -> "Original record hashCode: " + original.hashCode());


    // 3. Use maxSizeOf to create a ByteBuffer
    final int maxSize = pickler.maxSizeOf(original);
    LOGGER.fine(() -> "maxSizeOf: " + maxSize);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSize);

    // 4. Write the record to the buffer
    final int bytesWritten = pickler.serialize(buffer, original);
    LOGGER.fine(() -> "Bytes written: " + bytesWritten);


    // 5. Flip the buffer for reading
    buffer.flip();

    // 6. Read the record back
    final RecordWithEnum deserialized = pickler.deserialize(buffer);
    LOGGER.fine(() -> "Deserialized record: " + deserialized);

    // 7. Assert the deserialized record is equal to the original
    assertEquals(original, deserialized);
  }

  @Test
  void testRecordWithEmptyRoundTrip() {
    // 1. Get a pickler for the trivial record
    final Pickler<RecordWithEmpty> pickler = Pickler.forClass(RecordWithEmpty.class);

    // 2. Create an instance of the record
    final var original = new RecordWithEmpty(10, new EmptyRecord());
    LOGGER.fine(() -> "Original record: " + original);
    LOGGER.fine(() -> "Original record hashCode: " + original.hashCode());


    // 3. Use maxSizeOf to create a ByteBuffer
    final int maxSize = pickler.maxSizeOf(original);
    LOGGER.fine(() -> "maxSizeOf: " + maxSize);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSize);

    // 4. Write the record to the buffer
    final int bytesWritten = pickler.serialize(buffer, original);
    LOGGER.fine(() -> "Bytes written: " + bytesWritten);


    // 5. Flip the buffer for reading
    buffer.flip();

    // 6. Read the record back
    final RecordWithEmpty deserialized = pickler.deserialize(buffer);
    LOGGER.fine(() -> "Deserialized record: " + deserialized);

    // 7. Assert the deserialized record is equal to the original
    assertEquals(original, deserialized);
  }

  public record GenRecord_557(java.util.Map<java.lang.Boolean[], java.lang.String> value) {
  }

  @Test
  void testGenRecord_557() {
    final Pickler<GenRecord_557> pickler = Pickler.forClass(GenRecord_557.class);
    final GenRecord_557 original = new GenRecord_557(Map.of(new Boolean[]{true, false}, "test"));
    LOGGER.fine(() -> "Original record: " + original);
    LOGGER.fine(() -> "Original record hashCode: " + original.hashCode());

    final ByteBuffer buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    pickler.serialize(buffer, original);
    buffer.flip();

    final GenRecord_557 deserialized = pickler.deserialize(buffer);
    LOGGER.fine(() -> "Deserialized record: " + deserialized);

    assertEquals(original.value().size(), deserialized.value().size());
    //noinspection OptionalGetWithoutIsPresent
    assertThat(deserialized.value().keySet().stream().findFirst().get()).containsExactly(true, false);
    assertEquals(original.value().get(new Boolean[]{true, false}), deserialized.value().get(new Boolean[]{true, false}));
  }

  public record GenRecord_MapArrayBooleanToString(java.util.Map<java.lang.Boolean[], java.lang.String> value) {
  }

  @Test
  void testGenRecord_MapArrayBooleanToString() {
    final Pickler<GenRecord_MapArrayBooleanToString> pickler = Pickler.forClass(GenRecord_MapArrayBooleanToString.class);
    final GenRecord_MapArrayBooleanToString original = new GenRecord_MapArrayBooleanToString(Map.of(new Boolean[]{true, false}, "test"));
    LOGGER.fine(() -> "Original record: " + original);
    LOGGER.fine(() -> "Original record hashCode: " + original.hashCode());

    final ByteBuffer buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    pickler.serialize(buffer, original);
    buffer.flip();

    final GenRecord_MapArrayBooleanToString deserialized = pickler.deserialize(buffer);
    LOGGER.fine(() -> "Deserialized record: " + deserialized);

    assertEquals(original.value().size(), deserialized.value().size());
    //noinspection OptionalGetWithoutIsPresent
    assertThat(deserialized.value().keySet().stream().findFirst().get()).containsExactly(true, false);
    assertEquals(original.value().get(new Boolean[]{true, false}), deserialized.value().get(new Boolean[]{true, false}));
  }

}

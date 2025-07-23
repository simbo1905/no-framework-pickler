// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class OptionalTypesTest {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  // 4. OptionalTypesTest
  // Purpose: Test Optional with different contents
  public record SimpleOptionalsRecord(Optional<String> str, Optional<Integer> i) {
  }

  public record NestedOptionalsRecord(Optional<Optional<String>> nested) {
  }

  public record OptionalsOfContainersRecord(Optional<String[]> array, Optional<List<Integer>> list) {
  }

  public record ContainersOfOptionalsRecord(Optional<String>[] array, List<Optional<Integer>> list) {
  }

  @Test
  void testSimpleOptionals() {
    final var pickler = Pickler.forClass(SimpleOptionalsRecord.class);

    final var record1 = new SimpleOptionalsRecord(Optional.of("a"), Optional.of(1));
    final var maxSizeOfRecord1 = pickler.maxSizeOf(record1);
    final ByteBuffer buffer1 = ByteBuffer.allocate(maxSizeOfRecord1 + 64);
    final int bytesWritten1 = pickler.serialize(buffer1, record1);
    assertThat(bytesWritten1).isLessThanOrEqualTo(maxSizeOfRecord1);
    buffer1.flip();
    final var deserialized1 = pickler.deserialize(buffer1);
    assertThat(deserialized1).isEqualTo(record1);

    final var record2 = new SimpleOptionalsRecord(Optional.empty(), Optional.empty());
    final var maxSizeOfRecord2 = pickler.maxSizeOf(record2);
    final ByteBuffer buffer2 = ByteBuffer.allocate(maxSizeOfRecord2 + 64);
    final int bytesWritten2 = pickler.serialize(buffer2, record2);
    assertThat(bytesWritten2).isLessThanOrEqualTo(maxSizeOfRecord2);
    buffer2.flip();
    final var deserialized2 = pickler.deserialize(buffer2);
    assertThat(deserialized2).isEqualTo(record2);
  }

  @Test
  void testNestedOptionals() {
    final var pickler = Pickler.forClass(NestedOptionalsRecord.class);

    final var record1 = new NestedOptionalsRecord(Optional.of(Optional.of("a")));
    final var maxSizeOfRecord1 = pickler.maxSizeOf(record1);
    final ByteBuffer buffer1 = ByteBuffer.allocate(maxSizeOfRecord1 + 64);
    final int bytesWritten1 = pickler.serialize(buffer1, record1);
    assertThat(bytesWritten1).isLessThanOrEqualTo(maxSizeOfRecord1);
    buffer1.flip();
    final var deserialized1 = pickler.deserialize(buffer1);
    assertThat(deserialized1).isEqualTo(record1);

    final var record2 = new NestedOptionalsRecord(Optional.of(Optional.empty()));
    final var maxSizeOfRecord2 = pickler.maxSizeOf(record2);
    final ByteBuffer buffer2 = ByteBuffer.allocate(maxSizeOfRecord2 + 64);
    final int bytesWritten2 = pickler.serialize(buffer2, record2);
    assertThat(bytesWritten2).isLessThanOrEqualTo(maxSizeOfRecord2);
    buffer2.flip();
    final var deserialized2 = pickler.deserialize(buffer2);
    assertThat(deserialized2).isEqualTo(record2);

    final var record3 = new NestedOptionalsRecord(Optional.empty());
    final var maxSizeOfRecord3 = pickler.maxSizeOf(record3);
    final ByteBuffer buffer3 = ByteBuffer.allocate(maxSizeOfRecord3 + 64);
    final int bytesWritten3 = pickler.serialize(buffer3, record3);
    assertThat(bytesWritten3).isLessThanOrEqualTo(maxSizeOfRecord3);
    buffer3.flip();
    final var deserialized3 = pickler.deserialize(buffer3);
    assertThat(deserialized3).isEqualTo(record3);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  void testOptionalsOfContainers() {
    final var pickler = Pickler.forClass(OptionalsOfContainersRecord.class);

    final var record1 = new OptionalsOfContainersRecord(Optional.of(new String[]{"a", "b"}), Optional.of(List.of(1, 2)));
    final var maxSizeOfRecord1 = pickler.maxSizeOf(record1);
    final ByteBuffer buffer1 = ByteBuffer.allocate(maxSizeOfRecord1 + 64);
    final int bytesWritten1 = pickler.serialize(buffer1, record1);
    assertThat(bytesWritten1).isLessThanOrEqualTo(maxSizeOfRecord1);
    buffer1.flip();
    final var deserialized1 = pickler.deserialize(buffer1);
    assertThat(deserialized1.list()).isEqualTo(record1.list());
    assertThat(deserialized1.array()).hasValueSatisfying(a -> assertThat(a).containsExactly(record1.array().get()));

    final var record2 = new OptionalsOfContainersRecord(Optional.empty(), Optional.empty());
    final var maxSizeOfRecord2 = pickler.maxSizeOf(record2);
    final ByteBuffer buffer2 = ByteBuffer.allocate(maxSizeOfRecord2 + 64);
    final int bytesWritten2 = pickler.serialize(buffer2, record2);
    assertThat(bytesWritten2).isLessThanOrEqualTo(maxSizeOfRecord2);
    buffer2.flip();
    final var deserialized2 = pickler.deserialize(buffer2);
    assertThat(deserialized2).isEqualTo(record2);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void testContainersOfOptionals() {
    final var record = new ContainersOfOptionalsRecord(
        new Optional[]{Optional.of("a"), Optional.empty(), Optional.of("c")},
        List.of(Optional.of(1), Optional.empty(), Optional.of(3))
    );
    final var pickler = Pickler.forClass(ContainersOfOptionalsRecord.class);
    final var maxSizeOfRecord = pickler.maxSizeOf(record);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
    final int bytesWritten = pickler.serialize(buffer, record);
    assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized.list()).isEqualTo(record.list());
    assertThat(deserialized.array()).containsExactly(record.array());
  }
}

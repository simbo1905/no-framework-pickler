// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/// Tests for enum constants in sealed interfaces - reproduces Paxos benchmark bug
public class EnumConstantTests {
  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  /// Simple enum to test serialization
  public enum Operation {
    NOOP, READ
  }

  /// Another enum type to test multiple enums
  public enum Priority {
    LOW, MEDIUM, HIGH
  }

  /// Sealed interface with enum constant (reproduces bug pattern)
  public sealed interface Command permits NoOperation, DataCommand {
  }

  /// Record implementing sealed interface with enum constant
  public record NoOperation(Operation op) implements Command {
  }

  /// Record with data implementing sealed interface
  public record DataCommand(byte[] data, Operation op) implements Command {

  }

  /// Record with multiple enum types
  public record MultiEnumRecord(Operation op, Priority priority) {

  }

  static final NoOperation NOOP = new NoOperation(Operation.NOOP);

  @Test
  void testEnumConstantSerialization() {
    final var pickler = Pickler.forClass(Command.class);
    final var buffer = ByteBuffer.allocate(1024);
    pickler.serialize(buffer, NOOP);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized)
        .isInstanceOf(NoOperation.class)
        .extracting("op")
        .isEqualTo(Operation.NOOP);
  }

  @Test
  void testEnumInRecordSerialization() {
    final var pickler = Pickler.forClass(NoOperation.class);
    final var operation = new NoOperation(Operation.READ);

    assertDoesNotThrow(() -> {
      final var buffer = ByteBuffer.allocate(256);
      pickler.serialize(buffer, operation);
    }, "Enum in record serialization should work");
  }

  @Test
  void testMultipleEnumsInRecord() {
    final var pickler = Pickler.forClass(MultiEnumRecord.class);
    final var record = new MultiEnumRecord(Operation.READ, Priority.HIGH);

    final var buffer = ByteBuffer.allocate(256);
    pickler.serialize(buffer, record);
    buffer.flip();
    
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(record);
    assertThat(deserialized.op()).isEqualTo(Operation.READ);
    assertThat(deserialized.priority()).isEqualTo(Priority.HIGH);
  }
}

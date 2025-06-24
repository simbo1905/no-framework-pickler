// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/// Tests for enum constants in sealed interfaces - reproduces Paxos benchmark bug
class EnumConstantTests {

  /// Simple enum to test serialization
  enum Operation {
    NOOP, READ
  }

  /// Sealed interface with enum constant (reproduces bug pattern)
  sealed interface Command permits NoOperation, DataCommand {
  }

  /// Record implementing sealed interface with enum constant
  record NoOperation(Operation op) implements Command {
  }

  /// Record with data implementing sealed interface
  record DataCommand(byte[] data, Operation op) implements Command {

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
}

// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905;

import com.github.trex_paxos.AbstractCommand;
import com.github.trex_paxos.BallotNumber;
import com.github.trex_paxos.Command;
import com.github.trex_paxos.NoOperation;
import com.github.trex_paxos.msg.Accept;
import io.github.simbo1905.no.framework.Pickler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

public class PaxosTests {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  static final Accept[] original = {
      new Accept((short) 1, 2L, new BallotNumber((short) 3, 4, (short) 5), NoOperation.NOOP),
      new Accept((short) 6, 7L, new BallotNumber((short) 8, 9, (short) 10), new Command("data".getBytes(StandardCharsets.UTF_8), (byte) 11)),
  };

  @Test
  void testPaxosAccepts() {
    final ByteBuffer readyToReadBack;
    final var writeBuffer = ByteBuffer.allocate(2048); // Allocate a buffer for writing
    final var pickler = Pickler.forClass(Accept.class);
    for (var accept : original) {
      pickler.serialize(writeBuffer, accept); // Serialize each Accept record into the buffer
    }
    readyToReadBack = writeBuffer.flip(); // Prepare the buffer for reading

    final var readBuffer = readyToReadBack; // Allocate a buffer for reading
    IntStream.range(0, original.length).forEach(i -> {
      final var deserialized = pickler.deserialize(readBuffer); // Deserialize each Accept record from the buffer
      assert deserialized.equals(original[i]); // Verify that the deserialized record matches the original
    });
  }

  @Test
  void testAcceptNoop() {
    // Test serialization of Accept with NoOperation
    final var pickler = Pickler.forClass(Accept.class);
    final var buffer = ByteBuffer.allocate(1024);
    pickler.serialize(buffer, original[0]); // Serialize the first Accept record
    buffer.flip(); // Prepare the buffer for reading
    final var deserialized = pickler.deserialize(buffer); // Deserialize the Accept record
    assert deserialized.equals(original[0]); // Verify that the deserialized record matches the original
  }

  public record EnumTestRecord(AbstractCommand command) {
  }

  @Test
  void testNoOp() {
    // Test serialization of NoOperation using EnumTestRecord
    final var pickler = Pickler.forClass(EnumTestRecord.class);
    final var buffer = ByteBuffer.allocate(1024);
    final var testWithNoOp = new EnumTestRecord(NoOperation.NOOP);
    pickler.serialize(buffer, testWithNoOp); // Serialize the record containing NoOperation
    buffer.flip(); // Prepare the buffer for reading
    final var deserialized = pickler.deserialize(buffer); // Deserialize the record
    assert deserialized.equals(testWithNoOp); // Verify that the deserialized record matches the original
  }
}

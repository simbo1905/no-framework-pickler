// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class UserTypesTest {

    @BeforeAll
    static void setupLogging() {
        LoggingControl.setupCleanLogging();
    }

    // 6. UserTypesTest
    // Purpose: Test enums, records, interfaces, and recursive structures
    public enum Priority {LOW, MEDIUM, HIGH}

    public record ComplexRecord(String name, int value) {
    }

    public sealed interface Animal permits Dog, Cat {
    }

    public record Dog(String name) implements Animal {
    }

    public record Cat(String name) implements Animal {
    }

    public record EnumTypesRecord(ArrayTypesTest.Color color, Priority priority) {
    }

    public record RecordTypesRecord(ArrayTypesTest.SimpleRecord simple, ComplexRecord complex) {
    }

    public record InterfaceTypesRecord(Animal animal) {
    }

    // Recursive record for tree structure
    public record TreeNode(String value, TreeNode left, TreeNode right) {
    }

    public record RecursiveTypesRecord(TreeNode tree) {
    }


    @Test
    void testEnumTypes() {
        final var record = new EnumTypesRecord(ArrayTypesTest.Color.GREEN, Priority.HIGH);
        final var pickler = Pickler.forClass(EnumTypesRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }

    @Test
    void testRecordTypes() {
        final var record = new RecordTypesRecord(new ArrayTypesTest.SimpleRecord(123), new ComplexRecord("test", 456));
        final var pickler = Pickler.forClass(RecordTypesRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }

    @Test
    void testInterfaceTypes() {
        final var pickler = Pickler.forClass(InterfaceTypesRecord.class);

        final var record1 = new InterfaceTypesRecord(new Dog("Fido"));
        final var maxSizeOfRecord1 = pickler.maxSizeOf(record1);
        final ByteBuffer buffer1 = ByteBuffer.allocate(maxSizeOfRecord1 + 64);
        final int bytesWritten1 = pickler.serialize(buffer1, record1);
        assertThat(bytesWritten1).isLessThanOrEqualTo(maxSizeOfRecord1);
        buffer1.flip();
        final var deserialized1 = pickler.deserialize(buffer1);
        assertThat(deserialized1).isEqualTo(record1);

        final var record2 = new InterfaceTypesRecord(new Cat("Whiskers"));
        final var maxSizeOfRecord2 = pickler.maxSizeOf(record2);
        final ByteBuffer buffer2 = ByteBuffer.allocate(maxSizeOfRecord2 + 64);
        final int bytesWritten2 = pickler.serialize(buffer2, record2);
        assertThat(bytesWritten2).isLessThanOrEqualTo(maxSizeOfRecord2);
        buffer2.flip();
        final var deserialized2 = pickler.deserialize(buffer2);
        assertThat(deserialized2).isEqualTo(record2);
    }

    @Test
    void testRecursiveTypes() {
        final var tree = new TreeNode("root", new TreeNode("left", null, null), new TreeNode("right", null, null));
        final var record = new RecursiveTypesRecord(tree);
        final var pickler = Pickler.forClass(RecursiveTypesRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }
}

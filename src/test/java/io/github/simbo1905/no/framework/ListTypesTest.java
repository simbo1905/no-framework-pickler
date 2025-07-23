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

public class ListTypesTest {

    @BeforeAll
    static void setupLogging() {
        LoggingControl.setupCleanLogging();
    }

    // 3. ListTypesTest
    // Purpose: Test lists with different element types and nesting
    public record SimpleListsRecord(List<String> strings, List<Integer> ints) {
    }

    public record NestedListsRecord(List<List<String>> nested) {
    }

    public record ListsOfContainersRecord(List<int[]> arrayList, List<Optional<String>> optionalList) {
    }

    @Test
    void testSimpleLists() {
        final var record = new SimpleListsRecord(List.of("a", "b"), List.of(1, 2, 3));
        final var pickler = Pickler.forClass(SimpleListsRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }

    @Test
    void testNestedLists() {
        final var record = new NestedListsRecord(List.of(List.of("a", "b"), List.of("c", "d")));
        final var pickler = Pickler.forClass(NestedListsRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }

    @Test
    void testListsOfContainers() {
        final var record = new ListsOfContainersRecord(
                List.of(new int[]{1, 2}, new int[]{3, 4}),
                List.of(Optional.of("a"), Optional.empty(), Optional.of("c"))
        );
        final var pickler = Pickler.forClass(ListsOfContainersRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized.optionalList()).isEqualTo(record.optionalList());
        assertThat(deserialized.arrayList()).hasSameSizeAs(record.arrayList());
        for (int i = 0; i < deserialized.arrayList().size(); i++) {
            assertThat(deserialized.arrayList().get(i)).isEqualTo(record.arrayList().get(i));
        }
    }
}

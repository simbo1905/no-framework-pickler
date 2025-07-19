// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class MapTypesTest {

    @BeforeAll
    static void setupLogging() {
        LoggingControl.setupCleanLogging();
    }

    // 5. MapTypesTest
    // Purpose: Test maps systematically by key/value type categories
    public record PrimitiveKeyMapsRecord(Map<Integer, String> intKey, Map<Boolean, Double> boolKey) {
    }

    public record BoxedKeyMapsRecord(Map<Integer, String> boxedIntKey, Map<Boolean, Double> boxedBoolKey) {
    }

    public record ReferenceKeyMapsRecord(Map<String, Integer> stringKey, Map<LocalDate, String> dateKey) {
    }

    public record EnumKeyMapsRecord(Map<ArrayTypesTest.Color, String> enumKey) {
    }

    public record ArrayKeyMapsRecord(Map<int[], String> primitiveArrayKey, Map<String[], Integer> refArrayKey) {
    }

    public record NestedMapsRecord(Map<String, Map<Integer, Double>> nested) {
    }

    public record MapsOfContainersRecord(Map<String, List<Integer>> mapOfList, Map<Integer, Optional<String>> mapOfOptional) {
    }

    public record ContainersOfMapsRecord(List<Map<String, Integer>> listOfMaps, Optional<Map<String, Integer>> optionalMap) {
    }

    @Test
    void testPrimitiveKeyMaps() {
        final var record = new PrimitiveKeyMapsRecord(Map.of(1, "a", 2, "b"), Map.of(true, 1.0, false, 2.0));
        final var pickler = Pickler.forClass(PrimitiveKeyMapsRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }

    @Test
    void testBoxedKeyMaps() {
        final var record = new BoxedKeyMapsRecord(Map.of(1, "a", 2, "b"), Map.of(true, 1.0, false, 2.0));
        final var pickler = Pickler.forClass(BoxedKeyMapsRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }

    @Test
    void testReferenceKeyMaps() {
        final var record = new ReferenceKeyMapsRecord(Map.of("a", 1, "b", 2), Map.of(LocalDate.now(), "today"));
        final var pickler = Pickler.forClass(ReferenceKeyMapsRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }

    @Test
    void testEnumKeyMaps() {
        final var record = new EnumKeyMapsRecord(Map.of(ArrayTypesTest.Color.RED, "red", ArrayTypesTest.Color.BLUE, "blue"));
        final var pickler = Pickler.forClass(EnumKeyMapsRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }

    @Test
    void testArrayKeyMaps() {
        final var record = new ArrayKeyMapsRecord(Map.of(new int[]{1, 2}, "a"), Map.of(new String[]{"a", "b"}, 1));
        final var pickler = Pickler.forClass(ArrayKeyMapsRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized.primitiveArrayKey().keySet().iterator().next()).isEqualTo(record.primitiveArrayKey().keySet().iterator().next());
        assertThat(deserialized.refArrayKey().keySet().iterator().next()).isEqualTo(record.refArrayKey().keySet().iterator().next());
    }

    @Test
    void testNestedMaps() {
        final var record = new NestedMapsRecord(Map.of("a", Map.of(1, 1.0, 2, 2.0)));
        final var pickler = Pickler.forClass(NestedMapsRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }

    @Test
    void testMapsOfContainers() {
        final var record = new MapsOfContainersRecord(Map.of("a", List.of(1, 2)), Map.of(1, Optional.of("a")));
        final var pickler = Pickler.forClass(MapsOfContainersRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }

    @Test
    void testContainersOfMaps() {
        final var record = new ContainersOfMapsRecord(List.of(Map.of("a", 1)), Optional.of(Map.of("b", 2)));
        final var pickler = Pickler.forClass(ContainersOfMapsRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }
}

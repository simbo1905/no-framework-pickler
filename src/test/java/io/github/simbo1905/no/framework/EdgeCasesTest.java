// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class EdgeCasesTest {

    @BeforeAll
    static void setupLogging() {
        LoggingControl.setupCleanLogging();
    }

    // 7. EdgeCasesTest
    // Purpose: Test boundary conditions and special cases
    public record EmptyTypesRecord() {
    }

    public record LargeStructureRecord(int[] largeArray, Map<String, List<Double>> largeNested) {
    }

    public record SpecialValuesRecord(float[] specialFloats, double[] specialDoubles) {
    }

    @Test
    void testEmptyTypes() {
        final var record = new EmptyTypesRecord();
        final var pickler = Pickler.forClass(EmptyTypesRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized).isEqualTo(record);
    }

    @Test
    void testLargeStructure() {
        int[] largeArray = IntStream.range(0, 1000).toArray();
        Map<String, List<Double>> largeNested = Map.of(
                "key1", List.of(1.0, 2.0, 3.0, 4.0, 5.0),
                "key2", List.of(6.0, 7.0, 8.0, 9.0, 10.0)
        );
        final var record = new LargeStructureRecord(largeArray, largeNested);
        final var pickler = Pickler.forClass(LargeStructureRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized.largeArray()).isEqualTo(record.largeArray());
        assertThat(deserialized.largeNested()).isEqualTo(record.largeNested());
    }

    @Test
    void testSpecialValues() {
        float[] specialFloats = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.MIN_VALUE, Float.MAX_VALUE};
        double[] specialDoubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.MIN_VALUE, Double.MAX_VALUE};
        final var record = new SpecialValuesRecord(specialFloats, specialDoubles);
        final var pickler = Pickler.forClass(SpecialValuesRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized.specialFloats()).containsExactly(record.specialFloats());
        assertThat(deserialized.specialDoubles()).containsExactly(record.specialDoubles());
    }
}

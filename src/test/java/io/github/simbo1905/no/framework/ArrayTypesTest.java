// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

public class ArrayTypesTest {

    @BeforeAll
    static void setupLogging() {
        LoggingControl.setupCleanLogging();
    }

    public enum Color {RED, GREEN, BLUE}

    public record SimpleRecord(int i) {
    }

    // 2. ArrayTypesTest
    // Purpose: Test arrays systematically by element type category
    public record PrimitiveArraysRecord(boolean[] bools, int[] ints, double[] doubles) {
    }

    public record BoxedArraysRecord(Boolean[] bools, Integer[] ints, Double[] doubles) {
    }

    public record ReferenceArraysRecord(String[] strings, LocalDate[] dates) {
    }

    public record UserTypeArraysRecord(Color[] enums, SimpleRecord[] records) {
    }

    public record NestedArraysRecord(int[][] matrix2d, String[][][] matrix3d) {
    }

    public record NullElementArraysRecord(String[] withNulls, Integer[] withNulls2) {
    }


    @Test
    void testPrimitiveArrays() {
        final var record = new PrimitiveArraysRecord(new boolean[]{true, false}, new int[]{1, 2, 3}, new double[]{1.0, 2.0, 3.0});
        final var pickler = Pickler.forClass(PrimitiveArraysRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized.bools()).isEqualTo(record.bools());
        assertThat(deserialized.ints()).isEqualTo(record.ints());
        assertThat(deserialized.doubles()).isEqualTo(record.doubles());
    }

    @Test
    void testBoxedArrays() {
        final var record = new BoxedArraysRecord(new Boolean[]{true, false}, new Integer[]{1, 2, 3}, new Double[]{1.0, 2.0, 3.0});
        final var pickler = Pickler.forClass(BoxedArraysRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized.bools()).containsExactly(record.bools());
        assertThat(deserialized.ints()).containsExactly(record.ints());
        assertThat(deserialized.doubles()).containsExactly(record.doubles());
    }

    @Test
    void testReferenceArrays() {
        final var record = new ReferenceArraysRecord(new String[]{"a", "b", "c"}, new LocalDate[]{LocalDate.now(), LocalDate.now()});
        final var pickler = Pickler.forClass(ReferenceArraysRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized.strings()).containsExactly(record.strings());
        assertThat(deserialized.dates()).containsExactly(record.dates());
    }

    @Test
    void testUserTypeArrays() {
        final var record = new UserTypeArraysRecord(new Color[]{Color.RED, Color.BLUE}, new SimpleRecord[]{new SimpleRecord(1), new SimpleRecord(2)});
        final var pickler = Pickler.forClass(UserTypeArraysRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized.enums()).containsExactly(record.enums());
        assertThat(deserialized.records()).containsExactly(record.records());
    }

    @Test
    void testNestedArrays() {
        final var record = new NestedArraysRecord(new int[][]{{1, 2}, {3, 4}}, new String[][][]{{{"a"}, {"b"}}, {{"c"}, {"d"}}});
        final var pickler = Pickler.forClass(NestedArraysRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized.matrix2d()).isDeepEqualTo(record.matrix2d());
        assertThat(deserialized.matrix3d()).isDeepEqualTo(record.matrix3d());
    }

    @Test
    void testNullElementArrays() {
        final var record = new NullElementArraysRecord(new String[]{"a", null, "c"}, new Integer[]{1, null, 3});
        final var pickler = Pickler.forClass(NullElementArraysRecord.class);
        final var maxSizeOfRecord = pickler.maxSizeOf(record);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
        final int bytesWritten = pickler.serialize(buffer, record);
        assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
        buffer.flip();
        final var deserialized = pickler.deserialize(buffer);
        assertThat(deserialized.withNulls()).containsExactly(record.withNulls());
        assertThat(deserialized.withNulls2()).containsExactly(record.withNulls2());
    }
}

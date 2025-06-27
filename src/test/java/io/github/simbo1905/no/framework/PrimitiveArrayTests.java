// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

/// Package-private tests for core machinery components
/// Tests the internal implementation details that are not part of the public API
public class PrimitiveArrayTests {

  static PrimitiveArrayRecord primitiveValueRecord =
      new PrimitiveArrayRecord(
          new boolean[]{true, false, true},
          new byte[]{1, 2, 3},
          new char[]{'a', 'b', 'c'},
          new short[]{1, 2, 3},
          new int[]{1, 2, 3},
          new long[]{1L, 2L, 3L},
          new float[]{1.0f, 2.0f, 3.0f},
          new double[]{1.0, 2.0, 3.0}
      );

  @BeforeAll
  static void setupLogging() {
    io.github.simbo1905.LoggingControl.setupCleanLogging();
  }

  @BeforeEach
  void setUp() {
    LOGGER.fine(() -> "Starting PrimitiveTests test");
  }

  @AfterEach
  void tearDown() {
    LOGGER.fine(() -> "Finished PrimitiveTests test");
  }

  @Test
  @DisplayName("Test primitive chains")
  void testPrimitives() {
    // Get the method handle for anIntNotZero()
    final @NotNull RecordComponent[] components = PrimitiveArrayRecord.class.getRecordComponents();
    // make an array of the component accessor methods
    final @NotNull MethodHandle[] accessors = new MethodHandle[components.length];
    final @NotNull TypeExpr[] typeExprs = new TypeExpr[components.length];

    IntStream.range(0, components.length).forEach(i -> {
      RecordComponent component = components[i];

      // Get accessor method handle
      try {
        MethodHandle accessor = MethodHandles.lookup().unreflect(component.getAccessor());

        // Log the method handle details
        LOGGER.fine(() -> String.format("Accessor method handle %d: %s for component %s",
            i, accessor.type(), component.getName()));

        accessors[i] = accessor;
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to un reflect accessor for " + component.getName(), e);
      }
      // Analyze the type of the component
      Type type = component.getGenericType();
      typeExprs[i] = TypeExpr.analyze(type);
    });

    // Test boolean component
    final TypeExpr typeExpr0 = typeExprs[0];
    final MethodHandle typeExpr0Accessor = accessors[0];
    testBooleanArrayRoundTrip(typeExpr0, typeExpr0Accessor);

    // Test byte component
    final TypeExpr typeExpr1 = typeExprs[1];
    final MethodHandle typeExpr1Accessor = accessors[1];
    testByteArrayRoundTrip(typeExpr1, typeExpr1Accessor);

    // Test char component
    final TypeExpr typeExpr2 = typeExprs[2];
    final MethodHandle typeExpr2Accessor = accessors[2];
    testCharArrayRoundTrip(typeExpr2, typeExpr2Accessor);

    // Test short component
    final TypeExpr typeExpr3 = typeExprs[3];
    final MethodHandle typeExpr3Accessor = accessors[3];
    testShortArrayRoundTrip(typeExpr3, typeExpr3Accessor);

    // Test int component
    final TypeExpr typeExpr4 = typeExprs[4];
    final MethodHandle typeExpr4Accessor = accessors[4];
    testIntArrayRoundTrip(typeExpr4, typeExpr4Accessor);

    // Test long component
    final TypeExpr typeExpr5 = typeExprs[5];
    final MethodHandle typeExpr5Accessor = accessors[5];
    testLongArrayRoundTrip(typeExpr5, typeExpr5Accessor);

    // Test float component
    final TypeExpr typeExpr6 = typeExprs[6];
    final MethodHandle typeExpr6Accessor = accessors[6];
    testFloatArrayRoundTrip(typeExpr6, typeExpr6Accessor);

    // Test double component
    final TypeExpr typeExpr7 = typeExprs[7];
    final MethodHandle typeExpr7Accessor = accessors[7];
    testDoubleArrayRoundTrip(typeExpr7, typeExpr7Accessor);
  }

  void testBooleanArrayRoundTrip(TypeExpr typeExpr0, MethodHandle typeExpr0Accessor) {
    LOGGER.fine(() -> "Type of first component: " + typeExpr0);
    // We expect the first component to be a primitive type of boolean
    assertThat(typeExpr0.isContainer()).isTrue();
    // switch on it being boolean
    if (typeExpr0 instanceof TypeExpr.ArrayNode(TypeExpr element)) {
      // boolean.class
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) element).type();
      final var writerChain = RecordPickler.buildPrimitiveArrayWriter(primitiveType, typeExpr0Accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write boolean value to buffer");
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        LOGGER.severe(() -> "Failed to write boolean value: " + e2.getMessage());
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      LOGGER.fine(() -> "Successfully wrote boolean value to buffer");
      // Now we can read it back

      final var readerChain = Companion.buildPrimitiveArrayReader(primitiveType);
      final var readValue = readerChain.apply(byteBuffer);
      LOGGER.fine(() -> "Read boolean value: " + readValue);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.booleanArray());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveArraySizer(primitiveType, typeExpr0Accessor);
      final int size = sizer.applyAsInt(primitiveValueRecord);
      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr0);
    }
  }

  void testByteArrayRoundTrip(TypeExpr typeExpr0, MethodHandle typeExpr0Accessor) {
    LOGGER.fine(() -> "Type of byte component: " + typeExpr0);
    if (typeExpr0 instanceof TypeExpr.ArrayNode(TypeExpr element)) {
      // boolean.class
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) element).type();
      final var writerChain = RecordPickler.buildPrimitiveArrayWriter(primitiveType, typeExpr0Accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      final var readerChain = Companion.buildPrimitiveArrayReader(primitiveType);
      final var readValue = readerChain.apply(byteBuffer);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.byteArray());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveArraySizer(primitiveType, typeExpr0Accessor);
      final int size = sizer.applyAsInt(primitiveValueRecord);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr0);
    }
  }

  void testCharArrayRoundTrip(TypeExpr typeExpr0, MethodHandle typeExpr0Accessor) {
    LOGGER.fine(() -> "Type of char component: " + typeExpr0);
    assertThat(typeExpr0.isContainer()).isTrue();
    if (typeExpr0 instanceof TypeExpr.ArrayNode(TypeExpr element)) {
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) element).type();
      final var writerChain = RecordPickler.buildPrimitiveArrayWriter(primitiveType, typeExpr0Accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      final var readerChain = Companion.buildPrimitiveArrayReader(primitiveType);
      final var readValue = readerChain.apply(byteBuffer);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.charArray());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveArraySizer(primitiveType, typeExpr0Accessor);
      final int size = sizer.applyAsInt(primitiveValueRecord);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr0);
    }
  }

  void testShortArrayRoundTrip(TypeExpr typeExpr0, MethodHandle typeExpr0Accessor) {
    LOGGER.fine(() -> "Type of short component: " + typeExpr0);
    assertThat(typeExpr0.isContainer()).isTrue();
    if (typeExpr0 instanceof TypeExpr.ArrayNode(TypeExpr element)) {
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) element).type();
      final var writerChain = RecordPickler.buildPrimitiveArrayWriter(primitiveType, typeExpr0Accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      final var readerChain = Companion.buildPrimitiveArrayReader(primitiveType);
      final var readValue = readerChain.apply(byteBuffer);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.shortArray());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveArraySizer(primitiveType, typeExpr0Accessor);
      final int size = sizer.applyAsInt(primitiveValueRecord);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr0);
    }
  }


  void testFloatArrayRoundTrip(TypeExpr typeExpr0, MethodHandle typeExpr0Accessor) {
    assertThat(typeExpr0.isContainer()).isTrue();
    if (typeExpr0 instanceof TypeExpr.ArrayNode(TypeExpr element)) {
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) element).type();
      final var writerChain = RecordPickler.buildPrimitiveArrayWriter(primitiveType, typeExpr0Accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      final var readerChain = Companion.buildPrimitiveArrayReader(primitiveType);
      final var readValue = readerChain.apply(byteBuffer);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.floatArray());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveArraySizer(primitiveType, typeExpr0Accessor);
      final int size = sizer.applyAsInt(primitiveValueRecord);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr0);
    }
  }

  void testDoubleArrayRoundTrip(TypeExpr typeExpr0, MethodHandle typeExpr0Accessor) {
    assertThat(typeExpr0.isContainer()).isTrue();
    if (typeExpr0 instanceof TypeExpr.ArrayNode(TypeExpr element)) {
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) element).type();
      final var writerChain = RecordPickler.buildPrimitiveArrayWriter(primitiveType, typeExpr0Accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      final var readerChain = Companion.buildPrimitiveArrayReader(primitiveType);
      final var readValue = readerChain.apply(byteBuffer);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.doubleArray());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveArraySizer(primitiveType, typeExpr0Accessor);
      final int size = sizer.applyAsInt(primitiveValueRecord);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr0);
    }
  }


  void testIntArrayRoundTrip(TypeExpr typeExpr0, MethodHandle typeExpr0Accessor) {
    LOGGER.fine(() -> "Type of int component: " + typeExpr0);
    assertThat(typeExpr0.isContainer()).isTrue();
    if (typeExpr0 instanceof TypeExpr.ArrayNode(TypeExpr element)) {
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) element).type();
      final var writerChain = RecordPickler.buildPrimitiveArrayWriter(primitiveType, typeExpr0Accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      final var readerChain = Companion.buildPrimitiveArrayReader(primitiveType);
      final var readValue = readerChain.apply(byteBuffer);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.intArray());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveArraySizer(primitiveType, typeExpr0Accessor);
      final int size = sizer.applyAsInt(primitiveValueRecord);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr0);
    }
  }

  void testLongArrayRoundTrip(TypeExpr typeExpr0, MethodHandle typeExpr0Accessor) {
    LOGGER.fine(() -> "Type of long component: " + typeExpr0);
    assertThat(typeExpr0.isContainer()).isTrue();
    if (typeExpr0 instanceof TypeExpr.ArrayNode(TypeExpr element)) {
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) element).type();
      final var writerChain = RecordPickler.buildPrimitiveArrayWriter(primitiveType, typeExpr0Accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      final var readerChain = Companion.buildPrimitiveArrayReader(primitiveType);
      final var readValue = readerChain.apply(byteBuffer);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.longArray());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveArraySizer(primitiveType, typeExpr0Accessor);
      final int size = sizer.applyAsInt(primitiveValueRecord);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr0);
    }
  }

  public record PrimitiveArrayRecord(
      boolean[] booleanArray,
      byte[] byteArray,
      char[] charArray,
      short[] shortArray,
      int[] intArray,
      long[] longArray,
      float[] floatArray,
      double[] doubleArray
  ) {
  }
}

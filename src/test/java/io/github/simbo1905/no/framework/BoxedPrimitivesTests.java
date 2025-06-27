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
import static io.github.simbo1905.no.framework.RecordPickler.buildValueSizer;
import static org.assertj.core.api.Assertions.assertThat;

/// Tests for core machinery components with boxed primitives
/// Tests the internal implementation details that are not part of the public API
public class BoxedPrimitivesTests {
  public record ValueRecord(
      Boolean booleanValue,
      Byte byteValue,
      Character charValue,
      Short shortValue,
      Integer intValue,
      Long longValue,
      Float floatValue,
      Double doubleValue
  ) {
  }

  static ValueRecord boxedValueRecord =
      new ValueRecord(true, (byte) 1, 'a', (short) 2, 3, 4L, 5.0f, 6.0);

  @BeforeAll
  static void setupLogging() {
    io.github.simbo1905.LoggingControl.setupCleanLogging();
  }

  @BeforeEach
  void setUp() {
    LOGGER.fine(() -> "Starting BoxedPrimitivesTests test");
  }

  @AfterEach
  void tearDown() {
    LOGGER.fine(() -> "Finished BoxedPrimitivesTests test");
  }

  @Test
  @DisplayName("Test boxed primitive chains")
  void testPrimitives() {
    // Get the method handle for anIntNotZero()
    final @NotNull RecordComponent[] components = ValueRecord.class.getRecordComponents();
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
    testBooleanRoundTrip(typeExpr0, typeExpr0Accessor);

    // Test byte component
    final TypeExpr typeExpr1 = typeExprs[1];
    final MethodHandle typeExpr1Accessor = accessors[1];
    testByteRoundTrip(typeExpr1, typeExpr1Accessor);

    // Test char component
    final TypeExpr typeExpr2 = typeExprs[2];
    final MethodHandle typeExpr2Accessor = accessors[2];
    testCharRoundTrip(typeExpr2, typeExpr2Accessor);

    // Test short component
    final TypeExpr typeExpr3 = typeExprs[3];
    final MethodHandle typeExpr3Accessor = accessors[3];
    testShortRoundTrip(typeExpr3, typeExpr3Accessor);

    // Test int component
    final TypeExpr typeExpr4 = typeExprs[4];
    final MethodHandle typeExpr4Accessor = accessors[4];
    testIntRoundTrip(typeExpr4, typeExpr4Accessor);

    // Test long component
    final TypeExpr typeExpr5 = typeExprs[5];
    final MethodHandle typeExpr5Accessor = accessors[5];
    testLongRoundTrip(typeExpr5, typeExpr5Accessor);

    // Test float component
    final TypeExpr typeExpr6 = typeExprs[6];
    final MethodHandle typeExpr6Accessor = accessors[6];
    testFloatRoundTrip(typeExpr6, typeExpr6Accessor);

    // Test double component
    final TypeExpr typeExpr7 = typeExprs[7];
    final MethodHandle typeExpr7Accessor = accessors[7];
    testDoubleRoundTrip(typeExpr7, typeExpr7Accessor);
  }

  void testByteRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of component: " + typeExpr);
    assertThat(typeExpr.isPrimitive()).isFalse();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine(() -> "Component is byte");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.BYTE);

      final var writer = Companion.buildValueWriter(node.type(), accessor);
      assertThat(writer).isNotNull();

      final var buffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write byte value to buffer");

      try {
        writer.accept(buffer, boxedValueRecord);
      } catch (Throwable e) {
        LOGGER.severe(() -> "Failed to write byte value: " + e.getMessage());
        throw new RuntimeException(e);
      }

      buffer.flip();
      LOGGER.fine(() -> "Successfully wrote byte value to buffer");

      final var reader = Companion.buildValueReader(node.type());
      final var result = reader.apply(buffer);

      LOGGER.fine(() -> "Read byte value: " + result);
      assertThat(result).isEqualTo(boxedValueRecord.byteValue());

      final int bytesWritten = buffer.position();
      final var sizer = buildValueSizer(node.type(), accessor);
      final int size = sizer.applyAsInt(boxedValueRecord);

      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testCharRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of component: " + typeExpr);
    assertThat(typeExpr.isPrimitive()).isFalse();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine(() -> "Component is char");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.CHARACTER);

      final var writer = Companion.buildValueWriter(node.type(), accessor);
      assertThat(writer).isNotNull();

      final var buffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write char value to buffer");

      try {
        writer.accept(buffer, boxedValueRecord);
      } catch (Throwable e) {
        LOGGER.severe(() -> "Failed to write char value: " + e.getMessage());
        throw new RuntimeException(e);
      }

      buffer.flip();
      LOGGER.fine(() -> "Successfully wrote char value to buffer");

      final var reader = Companion.buildValueReader(node.type());
      final var result = reader.apply(buffer);

      LOGGER.fine(() -> "Read char value: " + result);
      assertThat(result).isEqualTo(boxedValueRecord.charValue());

      final int bytesWritten = buffer.position();
      final var sizer = buildValueSizer(node.type(), accessor);
      final int size = sizer.applyAsInt(boxedValueRecord);

      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testShortRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of component: " + typeExpr);
    assertThat(typeExpr.isPrimitive()).isFalse();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine(() -> "Component is short");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.SHORT);

      final var writer = Companion.buildValueWriter(node.type(), accessor);
      assertThat(writer).isNotNull();

      final var buffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write short value to buffer");

      try {
        writer.accept(buffer, boxedValueRecord);
      } catch (Throwable e) {
        LOGGER.severe(() -> "Failed to write short value: " + e.getMessage());
        throw new RuntimeException(e);
      }

      buffer.flip();
      LOGGER.fine(() -> "Successfully wrote short value to buffer");

      final var reader = Companion.buildValueReader(node.type());
      final var result = reader.apply(buffer);

      LOGGER.fine(() -> "Read short value: " + result);
      assertThat(result).isEqualTo(boxedValueRecord.shortValue());

      final int bytesWritten = buffer.position();
      final var sizer = buildValueSizer(node.type(), accessor);
      final int size = sizer.applyAsInt(boxedValueRecord);

      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testIntRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of component: " + typeExpr);
    assertThat(typeExpr.isPrimitive()).isFalse();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine(() -> "Component is int");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.INTEGER);

      final var writer = Companion.buildValueWriter(node.type(), accessor);
      assertThat(writer).isNotNull();

      final var buffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write int value to buffer");

      try {
        writer.accept(buffer, boxedValueRecord);
      } catch (Throwable e) {
        LOGGER.severe(() -> "Failed to write int value: " + e.getMessage());
        throw new RuntimeException(e);
      }

      buffer.flip();
      LOGGER.fine(() -> "Successfully wrote int value to buffer");

      final var reader = Companion.buildValueReader(node.type());
      final var result = reader.apply(buffer);

      LOGGER.fine(() -> "Read int value: " + result);
      assertThat(result).isEqualTo(boxedValueRecord.intValue());

      final int bytesWritten = buffer.position();
      final var sizer = buildValueSizer(node.type(), accessor);
      final int size = sizer.applyAsInt(boxedValueRecord);

      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testLongRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of component: " + typeExpr);
    assertThat(typeExpr.isPrimitive()).isFalse();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine(() -> "Component is long");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.LONG);

      final var writer = Companion.buildValueWriter(node.type(), accessor);
      assertThat(writer).isNotNull();

      final var buffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write long value to buffer");

      try {
        writer.accept(buffer, boxedValueRecord);
      } catch (Throwable e) {
        LOGGER.severe(() -> "Failed to write long value: " + e.getMessage());
        throw new RuntimeException(e);
      }

      buffer.flip();
      LOGGER.fine(() -> "Successfully wrote long value to buffer");

      final var reader = Companion.buildValueReader(node.type());
      final var result = reader.apply(buffer);

      LOGGER.fine(() -> "Read long value: " + result);
      assertThat(result).isEqualTo(boxedValueRecord.longValue());

      final int bytesWritten = buffer.position();
      final var sizer = buildValueSizer(node.type(), accessor);
      final int size = sizer.applyAsInt(boxedValueRecord);

      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testFloatRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of component: " + typeExpr);
    assertThat(typeExpr.isPrimitive()).isFalse();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine(() -> "Component is float");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.FLOAT);

      final var writer = Companion.buildValueWriter(node.type(), accessor);
      assertThat(writer).isNotNull();

      final var buffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write float value to buffer");

      try {
        writer.accept(buffer, boxedValueRecord);
      } catch (Throwable e) {
        LOGGER.severe(() -> "Failed to write float value: " + e.getMessage());
        throw new RuntimeException(e);
      }

      buffer.flip();
      LOGGER.fine(() -> "Successfully wrote float value to buffer");

      final var reader = Companion.buildValueReader(node.type());
      final var result = reader.apply(buffer);

      LOGGER.fine(() -> "Read float value: " + result);
      assertThat(result).isEqualTo(boxedValueRecord.floatValue());

      final int bytesWritten = buffer.position();
      final var sizer = buildValueSizer(node.type(), accessor);
      final int size = sizer.applyAsInt(boxedValueRecord);

      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testDoubleRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of component: " + typeExpr);
    assertThat(typeExpr.isPrimitive()).isFalse();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine(() -> "Component is double");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.DOUBLE);

      final var writer = Companion.buildValueWriter(node.type(), accessor);
      assertThat(writer).isNotNull();

      final var buffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write double value to buffer");

      try {
        writer.accept(buffer, boxedValueRecord);
      } catch (Throwable e) {
        LOGGER.severe(() -> "Failed to write double value: " + e.getMessage());
        throw new RuntimeException(e);
      }

      buffer.flip();
      LOGGER.fine(() -> "Successfully wrote double value to buffer");

      final var reader = Companion.buildValueReader(node.type());
      final var result = reader.apply(buffer);

      LOGGER.fine(() -> "Read double value: " + result);
      assertThat(result).isEqualTo(boxedValueRecord.doubleValue());

      final int bytesWritten = buffer.position();
      final var sizer = buildValueSizer(node.type(), accessor);
      final int size = sizer.applyAsInt(boxedValueRecord);

      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testBooleanRoundTrip(TypeExpr typeExpr0, MethodHandle typeExpr0Accessor) {
    LOGGER.fine(() -> "Type of first component: " + typeExpr0);
    // We expect the first component to be a reference type of boolean
    assertThat(typeExpr0.isPrimitive()).isFalse();
    // switch on it being boolean
    if (typeExpr0 instanceof TypeExpr.RefValueNode e) {
      LOGGER.fine(() -> "First component is boolean");
      assertThat(e.type()).isEqualTo(TypeExpr.RefValueType.BOOLEAN);
      // Boolean.class
      final var writerChain = Companion.buildValueWriter(e.type(), typeExpr0Accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write boolean value to buffer");
      try {
        writerChain.accept(byteBuffer, boxedValueRecord);
      } catch (Throwable e2) {
        LOGGER.severe(() -> "Failed to write boolean value: " + e2.getMessage());
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      LOGGER.fine(() -> "Successfully wrote boolean value to buffer");
      // Now we can read it back
      final var readerChain = Companion.buildValueReader(e.type());
      final var readValue = readerChain.apply(byteBuffer);
      LOGGER.fine(() -> "Read boolean value: " + readValue);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(boxedValueRecord.booleanValue());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = buildValueSizer(e.type(), typeExpr0Accessor);
      final int size = sizer.applyAsInt(boxedValueRecord);
      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr0);
    }
  }

}

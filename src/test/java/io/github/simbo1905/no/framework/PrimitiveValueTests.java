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
public class PrimitiveValueTests {

  static PrimitiveValueRecord primitiveValueRecord =
      new PrimitiveValueRecord(true, (byte) 1, 'a', (short) 2, 3, 4L, 5.0f, 6.0);

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
    final @NotNull RecordComponent[] components = PrimitiveValueRecord.class.getRecordComponents();
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
      typeExprs[i] = TypeExpr.analyzeType(type);
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

  void testBooleanRoundTrip(TypeExpr typeExpr0, MethodHandle typeExpr0Accessor) {
    LOGGER.fine(() -> "Type of first component: " + typeExpr0);
    // We expect the first component to be a primitive type of boolean
    assertThat(typeExpr0.isPrimitive()).isTrue();
    // switch on it being boolean
    if (typeExpr0 instanceof TypeExpr.PrimitiveValueNode e) {
      LOGGER.fine(() -> "First component is boolean");
      assertThat(e.type()).isEqualTo(TypeExpr.PrimitiveValueType.BOOLEAN);
      // boolean.class
      final var writerChain = RecordPickler.buildPrimitiveValueWriter(e.type(), typeExpr0Accessor);
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
      final var readerChain = Companion.buildPrimitiveValueReader(e.type());
      final var readValue = readerChain.apply(byteBuffer);
      LOGGER.fine(() -> "Read boolean value: " + readValue);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.booleanValue());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveValueSizer(e.type());
      final int size = sizer.applyAsInt(primitiveValueRecord);
      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr0);
    }
  }

  void testByteRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of byte component: " + typeExpr);
    // We expect the component to be a primitive type of byte
    assertThat(typeExpr.isPrimitive()).isTrue();
    // switch on it being byte
    if (typeExpr instanceof TypeExpr.PrimitiveValueNode e) {
      LOGGER.fine(() -> "Byte component is byte");
      assertThat(e.type()).isEqualTo(TypeExpr.PrimitiveValueType.BYTE);
      // byte.class
      final var writerChain = RecordPickler.buildPrimitiveValueWriter(e.type(), accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write byte value to buffer");
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        LOGGER.severe(() -> "Failed to write byte value: " + e2.getMessage());
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      LOGGER.fine(() -> "Successfully wrote byte value to buffer");
      // Now we can read it back
      final var readerChain = Companion.buildPrimitiveValueReader(e.type());
      final var readValue = readerChain.apply(byteBuffer);
      LOGGER.fine(() -> "Read byte value: " + readValue);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.byteValue());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveValueSizer(e.type());
      final int size = sizer.applyAsInt(primitiveValueRecord);
      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testCharRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of char component: " + typeExpr);
    // We expect the component to be a primitive type of char
    assertThat(typeExpr.isPrimitive()).isTrue();
    // switch on it being char
    if (typeExpr instanceof TypeExpr.PrimitiveValueNode e) {
      LOGGER.fine(() -> "Char component is char");
      assertThat(e.type()).isEqualTo(TypeExpr.PrimitiveValueType.CHARACTER);
      // char.class
      final var writerChain = RecordPickler.buildPrimitiveValueWriter(e.type(), accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write char value to buffer");
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        LOGGER.severe(() -> "Failed to write char value: " + e2.getMessage());
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      LOGGER.fine(() -> "Successfully wrote char value to buffer");
      // Now we can read it back
      final var readerChain = Companion.buildPrimitiveValueReader(e.type());
      final var readValue = readerChain.apply(byteBuffer);
      LOGGER.fine(() -> "Read char value: " + readValue);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.charValue());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveValueSizer(e.type());
      final int size = sizer.applyAsInt(primitiveValueRecord);
      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testShortRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of short component: " + typeExpr);
    // We expect the component to be a primitive type of short
    assertThat(typeExpr.isPrimitive()).isTrue();
    // switch on it being short
    if (typeExpr instanceof TypeExpr.PrimitiveValueNode e) {
      LOGGER.fine(() -> "Short component is short");
      assertThat(e.type()).isEqualTo(TypeExpr.PrimitiveValueType.SHORT);
      // short.class
      final var writerChain = RecordPickler.buildPrimitiveValueWriter(e.type(), accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write short value to buffer");
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        LOGGER.severe(() -> "Failed to write short value: " + e2.getMessage());
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      LOGGER.fine(() -> "Successfully wrote short value to buffer");
      // Now we can read it back
      final var readerChain = Companion.buildPrimitiveValueReader(e.type());
      final var readValue = readerChain.apply(byteBuffer);
      LOGGER.fine(() -> "Read short value: " + readValue);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.shortValue());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveValueSizer(e.type());
      final int size = sizer.applyAsInt(primitiveValueRecord);
      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testIntRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of int component: " + typeExpr);
    // We expect the component to be a primitive type of int
    assertThat(typeExpr.isPrimitive()).isTrue();
    // switch on it being int
    if (typeExpr instanceof TypeExpr.PrimitiveValueNode e) {
      LOGGER.fine(() -> "Int component is int");
      assertThat(e.type()).isEqualTo(TypeExpr.PrimitiveValueType.INTEGER);
      // int.class
      final var writerChain = RecordPickler.buildPrimitiveValueWriter(e.type(), accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write int value to buffer");
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        LOGGER.severe(() -> "Failed to write int value: " + e2.getMessage());
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      LOGGER.fine(() -> "Successfully wrote int value to buffer");
      // Now we can read it back
      final var readerChain = Companion.buildPrimitiveValueReader(e.type());
      final var readValue = readerChain.apply(byteBuffer);
      LOGGER.fine(() -> "Read int value: " + readValue);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.intValue());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveValueSizer(e.type());
      final int size = sizer.applyAsInt(primitiveValueRecord);
      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testLongRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of long component: " + typeExpr);
    // We expect the component to be a primitive type of long
    assertThat(typeExpr.isPrimitive()).isTrue();
    // switch on it being long
    if (typeExpr instanceof TypeExpr.PrimitiveValueNode e) {
      LOGGER.fine(() -> "Long component is long");
      assertThat(e.type()).isEqualTo(TypeExpr.PrimitiveValueType.LONG);
      // long.class
      final var writerChain = RecordPickler.buildPrimitiveValueWriter(e.type(), accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write long value to buffer");
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        LOGGER.severe(() -> "Failed to write long value: " + e2.getMessage());
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      LOGGER.fine(() -> "Successfully wrote long value to buffer");
      // Now we can read it back
      final var readerChain = Companion.buildPrimitiveValueReader(e.type());
      final var readValue = readerChain.apply(byteBuffer);
      LOGGER.fine(() -> "Read long value: " + readValue);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.longValue());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveValueSizer(e.type());
      final int size = sizer.applyAsInt(primitiveValueRecord);
      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testFloatRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of float component: " + typeExpr);
    // We expect the component to be a primitive type of float
    assertThat(typeExpr.isPrimitive()).isTrue();
    // switch on it being float
    if (typeExpr instanceof TypeExpr.PrimitiveValueNode e) {
      LOGGER.fine(() -> "Float component is float");
      assertThat(e.type()).isEqualTo(TypeExpr.PrimitiveValueType.FLOAT);
      // float.class
      final var writerChain = RecordPickler.buildPrimitiveValueWriter(e.type(), accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write float value to buffer");
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        LOGGER.severe(() -> "Failed to write float value: " + e2.getMessage());
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      LOGGER.fine(() -> "Successfully wrote float value to buffer");
      // Now we can read it back
      final var readerChain = Companion.buildPrimitiveValueReader(e.type());
      final var readValue = readerChain.apply(byteBuffer);
      LOGGER.fine(() -> "Read float value: " + readValue);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.floatValue());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveValueSizer(e.type());
      final int size = sizer.applyAsInt(primitiveValueRecord);
      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testDoubleRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of double component: " + typeExpr);
    // We expect the component to be a primitive type of double
    assertThat(typeExpr.isPrimitive()).isTrue();
    // switch on it being double
    if (typeExpr instanceof TypeExpr.PrimitiveValueNode e) {
      LOGGER.fine(() -> "Double component is double");
      assertThat(e.type()).isEqualTo(TypeExpr.PrimitiveValueType.DOUBLE);
      // double.class
      final var writerChain = RecordPickler.buildPrimitiveValueWriter(e.type(), accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write double value to buffer");
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        LOGGER.severe(() -> "Failed to write double value: " + e2.getMessage());
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      LOGGER.fine(() -> "Successfully wrote double value to buffer");
      // Now we can read it back
      final var readerChain = Companion.buildPrimitiveValueReader(e.type());
      final var readValue = readerChain.apply(byteBuffer);
      LOGGER.fine(() -> "Read double value: " + readValue);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.doubleValue());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = Companion.buildPrimitiveValueSizer(e.type());
      final int size = sizer.applyAsInt(primitiveValueRecord);
      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  public record PrimitiveValueRecord(
      boolean booleanValue,
      byte byteValue,
      char charValue,
      short shortValue,
      int intValue,
      long longValue,
      float floatValue,
      double doubleValue
  ) {
  }
}

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
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static io.github.simbo1905.no.framework.RecordPickler.buildValueSizer;
import static org.assertj.core.api.Assertions.assertThat;

public class ReferenceValueTests {
  @SuppressWarnings("unused")
  public enum TestEnum {
    VALUE1, VALUE2, VALUE3
  }

  public record TestValuesRecord(
      UUID uuidValue,
      String stringValue,
      TestEnum enumValue
  ) {
  }

  static Map<Class<?>, PicklerImpl.TypeInfo> classTypeInfoMap = Map.of(
      TestEnum.class, new PicklerImpl.TypeInfo(1, 1L, TestEnum.values(), null),
      TestValuesRecord.class, new PicklerImpl.TypeInfo(2, 2L, null, null)
  );

  static TestValuesRecord referenceValueRecord =
      new TestValuesRecord(
          UUID.randomUUID(),
          "test-string",
          TestEnum.VALUE1
      );

  @BeforeAll
  static void setupLogging() {
    io.github.simbo1905.LoggingControl.setupCleanLogging();
  }

  @BeforeEach
  void setUp() {
    LOGGER.fine(() -> "Starting ReferenceValueTests test");
  }

  @AfterEach
  void tearDown() {
    LOGGER.fine(() -> "Finished ReferenceValueTests test");
  }

  @Test
  @DisplayName("Test reference value round trips")
  void testReferenceValues() {
    final @NotNull RecordComponent[] components = TestValuesRecord.class.getRecordComponents();
    final @NotNull MethodHandle[] accessors = new MethodHandle[components.length];
    final @NotNull TypeExpr[] typeExprs = new TypeExpr[components.length];

    IntStream.range(0, components.length).forEach(i -> {
      RecordComponent component = components[i];

      try {
        MethodHandle accessor = MethodHandles.lookup().unreflect(component.getAccessor());
        accessors[i] = accessor;

        Type type = component.getGenericType();
        typeExprs[i] = TypeExpr.analyze(type);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to unreflect accessor for " + component.getName(), e);
      }
    });

    testUuidRoundTrip(typeExprs[0], accessors[0]);
    testStringRoundTrip(typeExprs[1], accessors[1]);
    testEnumRoundTrip(typeExprs[2], accessors[2]);
  }

  void testUuidRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of UUID component: " + typeExpr);
    assertThat(typeExpr.isPrimitive()).isFalse();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine("Component is UUID");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.UUID);

      final var writer = Companion.buildValueWriter(node.type(), accessor);
      assertThat(writer).isNotNull();

      final var buffer = ByteBuffer.allocate(1024);
      LOGGER.fine("Attempting to write UUID to buffer");

      try {
        writer.accept(buffer, referenceValueRecord);
      } catch (Throwable e) {
        LOGGER.severe("Failed to write UUID: " + e.getMessage());
        throw new RuntimeException(e);
      }

      buffer.flip();
      LOGGER.fine("Successfully wrote UUID to buffer");

      final var reader = Companion.buildValueReader(node.type());
      final UUID result = (UUID) reader.apply(buffer);

      LOGGER.fine("Read UUID: " + result);
      assertThat(result).isEqualTo(referenceValueRecord.uuidValue());

      final int bytesWritten = buffer.position();
      final var sizer = buildValueSizer(node.type(), accessor);
      final int size = sizer.applyAsInt(referenceValueRecord);

      LOGGER.fine("Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testStringRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of String component: " + typeExpr);
    assertThat(typeExpr.isPrimitive()).isFalse();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine("Component is String");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.STRING);

      final var writer = Companion.buildValueWriter(node.type(), accessor);
      assertThat(writer).isNotNull();

      final var buffer = ByteBuffer.allocate(1024);
      LOGGER.fine("Attempting to write String to buffer");

      try {
        writer.accept(buffer, referenceValueRecord);
      } catch (Throwable e) {
        LOGGER.severe("Failed to write String: " + e.getMessage());
        throw new RuntimeException(e);
      }

      buffer.flip();
      LOGGER.fine("Successfully wrote String to buffer");

      final var reader = Companion.buildValueReader(node.type());
      final String result = (String) reader.apply(buffer);

      LOGGER.fine("Read String: " + result);
      assertThat(result).isEqualTo(referenceValueRecord.stringValue());

      final int bytesWritten = buffer.position();
      final var sizer = buildValueSizer(node.type(), accessor);
      final int size = sizer.applyAsInt(referenceValueRecord);

      LOGGER.fine("Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testEnumRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of Enum component: " + typeExpr);
    assertThat(typeExpr.isPrimitive()).isFalse();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine("Component is Enum");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.ENUM);

      final var writer = Companion.buildEnumWriter(classTypeInfoMap, accessor);
      assertThat(writer).isNotNull();

      final var buffer = ByteBuffer.allocate(1024);
      LOGGER.fine("Attempting to write Enum to buffer");

      try {
        writer.accept(buffer, referenceValueRecord);
      } catch (Throwable e) {
        LOGGER.severe("Failed to write Enum: " + e.getMessage());
        throw new RuntimeException(e.getMessage(), e);
      }

      buffer.flip();
      LOGGER.fine("Successfully wrote Enum to buffer");

      final var reader = Companion.buildEnumReader(classTypeInfoMap);
      final TestEnum result = (TestEnum) reader.apply(buffer);

      LOGGER.fine("Read Enum: " + result);
      assertThat(result).isEqualTo(referenceValueRecord.enumValue());

      final int bytesWritten = buffer.position();
      final var sizer = buildValueSizer(node.type(), accessor);
      final int size = sizer.applyAsInt(referenceValueRecord);

      LOGGER.fine("Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }
}

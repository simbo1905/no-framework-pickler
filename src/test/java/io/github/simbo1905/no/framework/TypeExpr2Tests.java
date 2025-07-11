// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("auxiliaryclass")
class TypeExpr2Tests {

  @Test
  void testPrimitiveTypes() {
    LOGGER.info(() -> "Testing primitive types...");
    // Test primitive types get negative markers
    var intExpr = TypeExpr2.analyzeType(int.class, List.of());
    assertThat(intExpr).isInstanceOf(TypeExpr2.PrimitiveValueNode.class);
    var intNode = (TypeExpr2.PrimitiveValueNode) intExpr;
    assertThat(intNode.marker()).isLessThan(0);
    assertThat(intNode.toTreeString()).isEqualTo("int");

    var boolExpr = TypeExpr2.analyzeType(boolean.class, List.of());
    assertThat(boolExpr).isInstanceOf(TypeExpr2.PrimitiveValueNode.class);
    var boolNode = (TypeExpr2.PrimitiveValueNode) boolExpr;
    assertThat(boolNode.marker()).isLessThan(0);
    assertThat(boolNode.toTreeString()).isEqualTo("boolean");
  }

  @Test
  void testBoxedPrimitives() {
    // Test boxed primitives get negative markers
    var integerExpr = TypeExpr2.analyzeType(Integer.class, List.of());
    assertThat(integerExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var integerNode = (TypeExpr2.RefValueNode) integerExpr;
    assertThat(integerNode.marker()).isLessThan(0);
    assertThat(integerNode.toTreeString()).isEqualTo("Integer");
  }

  @Test
  void testBuiltInValueTypes() {
    // Test String gets negative marker
    var stringExpr = TypeExpr2.analyzeType(String.class, List.of());
    assertThat(stringExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var stringNode = (TypeExpr2.RefValueNode) stringExpr;
    assertThat(stringNode.marker()).isLessThan(0);
    assertThat(stringNode.toTreeString()).isEqualTo("String");
  }

  @Test
  void testCustomTypeWithHandler() {
    // Create UUID handler with positive marker
    var uuidHandler = new SerdeHandler(
        UUID.class,
        1, // positive marker
        obj -> 16, // sizer
        (buffer, obj) -> {
        }, // writer
        buffer -> null // reader
    );

    var uuidExpr = TypeExpr2.analyzeType(UUID.class, List.of(uuidHandler));
    assertThat(uuidExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var uuidNode = (TypeExpr2.RefValueNode) uuidExpr;
    assertThat(uuidNode.marker()).isEqualTo(1);
    assertThat(uuidNode.type()).isEqualTo(TypeExpr2.RefValueType.CUSTOM);
    assertThat(uuidNode.toTreeString()).isEqualTo("UUID[m=1]");
  }

  @Test
  void testUserTypes() {
    // Test record gets marker 0 (uses type signature)
    var recordExpr = TypeExpr2.analyzeType(TestRecord.class, List.of());
    assertThat(recordExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var recordNode = (TypeExpr2.RefValueNode) recordExpr;
    assertThat(recordNode.marker()).isEqualTo(0);
    assertThat(recordNode.type()).isEqualTo(TypeExpr2.RefValueType.RECORD);
    assertThat(recordNode.toTreeString()).isEqualTo("TestRecord[sig]");

    // Test enum gets marker 0 (uses type signature)
    var enumExpr = TypeExpr2.analyzeType(TestEnum.class, List.of());
    assertThat(enumExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var enumNode = (TypeExpr2.RefValueNode) enumExpr;
    assertThat(enumNode.marker()).isEqualTo(0);
    assertThat(enumNode.type()).isEqualTo(TypeExpr2.RefValueType.ENUM);
    assertThat(enumNode.toTreeString()).isEqualTo("TestEnum[sig]");
  }

  @Test
  void testArrayTypes() {
    // Test primitive array
    var intArrayExpr = TypeExpr2.analyzeType(int[].class, List.of());
    assertThat(intArrayExpr).isInstanceOf(TypeExpr2.ArrayNode.class);
    var intArrayNode = (TypeExpr2.ArrayNode) intArrayExpr;
    assertThat(intArrayNode.toTreeString()).isEqualTo("ARRAY(int)");

    // Test reference array
    var stringArrayExpr = TypeExpr2.analyzeType(String[].class, List.of());
    assertThat(stringArrayExpr).isInstanceOf(TypeExpr2.ArrayNode.class);
    var stringArrayNode = (TypeExpr2.ArrayNode) stringArrayExpr;
    assertThat(stringArrayNode.toTreeString()).isEqualTo("ARRAY(String)");
  }

  @Test
  void testContainerTypes() {
    // Test List<String>
    record ListStringHolder(List<String> value) {
    }
    var components = ListStringHolder.class.getRecordComponents();
    var listType = components[0].getGenericType();

    var listExpr = TypeExpr2.analyzeType(listType, List.of());
    assertThat(listExpr).isInstanceOf(TypeExpr2.ListNode.class);
    assertThat(listExpr.toTreeString()).isEqualTo("LIST(String)");

    // Test Optional<Integer>
    record OptionalIntHolder(Optional<Integer> value) {
    }
    var optComponents = OptionalIntHolder.class.getRecordComponents();
    var optType = optComponents[0].getGenericType();

    var optExpr = TypeExpr2.analyzeType(optType, List.of());
    assertThat(optExpr).isInstanceOf(TypeExpr2.OptionalNode.class);
    assertThat(optExpr.toTreeString()).isEqualTo("OPTIONAL(Integer)");

    // Test Map<String, UUID> with custom UUID handler
    record MapHolder(Map<String, UUID> value) {
    }
    var mapComponents = MapHolder.class.getRecordComponents();
    var mapType = mapComponents[0].getGenericType();

    var uuidHandler = new SerdeHandler(
        UUID.class,
        1,
        obj -> 16,
        (buffer, obj) -> {
        },
        buffer -> null
    );

    var mapExpr = TypeExpr2.analyzeType(mapType, List.of(uuidHandler));
    assertThat(mapExpr).isInstanceOf(TypeExpr2.MapNode.class);
    assertThat(mapExpr.toTreeString()).isEqualTo("MAP(String,UUID[m=1])");
  }

  @Test
  void testNestedContainers() {
    // Test List<Optional<String>>
    record NestedHolder(List<Optional<String>> value) {
    }
    var components = NestedHolder.class.getRecordComponents();
    var nestedType = components[0].getGenericType();

    var nestedExpr = TypeExpr2.analyzeType(nestedType, List.of());
    assertThat(nestedExpr.toTreeString()).isEqualTo("LIST(OPTIONAL(String))");
  }

  @Test
  void testComponentSerdeBuilding() {
    // Test record with String and int components - must be public for reflection
    // Using the existing public TestRecord(String name, int value)

    // Build ComponentSerde array using Companion2
    ComponentSerde[] serdes = Companion2.buildComponentSerdes(
        TestRecord.class,
        List.of(), // No custom handlers for this test
        clazz -> obj -> 0, // Simple sizer resolver
        clazz -> (buffer, obj) -> {
        }, // Simple writer resolver
        clazz -> buffer -> null // Simple reader resolver
    );

    // Verify we have 2 ComponentSerdes (name and value)
    assertThat(serdes).hasSize(2);
    assertThat(serdes[0]).isNotNull();
    assertThat(serdes[1]).isNotNull();

    // Test serialization/deserialization of actual data
    TestRecord original = new TestRecord("hello", 42);
    ByteBuffer buffer = ByteBuffer.allocate(1024);

    // Test writing components
    serdes[0].writer().accept(buffer, original); // writes name
    serdes[1].writer().accept(buffer, original); // writes value

    // Reset buffer for reading
    buffer.flip();

    // Test reading components
    String readName = (String) serdes[0].reader().apply(buffer);
    int readValue = (int) serdes[1].reader().apply(buffer);

    assertThat(readName).isEqualTo("hello");
    assertThat(readValue).isEqualTo(42);
  }

  @Test
  void testSizerAlwaysGreaterThanOrEqualToWrittenBytes() {
    // Test that sizer always returns worst-case estimate >= actual bytes written

    // Build ComponentSerde array using Companion2
    ComponentSerde[] serdes = Companion2.buildComponentSerdes(
        TestRecord.class,
        List.of(), // No custom handlers for this test
        clazz -> obj -> 0, // Simple sizer resolver
        clazz -> (buffer, obj) -> {
        }, // Simple writer resolver
        clazz -> buffer -> null // Simple reader resolver
    );

    // Test various records
    TestRecord[] testCases = {
        new TestRecord("hello", 42),
        new TestRecord("", 0),
        new TestRecord("a very long string that should take more bytes", Integer.MAX_VALUE),
        new TestRecord(null, Integer.MIN_VALUE),
        new TestRecord("short", -1)
    };

    for (TestRecord record : testCases) {
      ByteBuffer buffer = ByteBuffer.allocate(1024);

      // Calculate sizes for each component
      for (int i = 0; i < serdes.length; i++) {
        ComponentSerde serde = serdes[i];

        // Get estimated size
        int estimatedSize = serde.sizer().applyAsInt(record);

        // Record position before writing
        int positionBefore = buffer.position();

        // Write the component
        serde.writer().accept(buffer, record);

        // Calculate actual bytes written
        int actualBytesWritten = buffer.position() - positionBefore;

        // Verify sizer is conservative (worst-case)
        assertThat(estimatedSize)
            .as("Component %d: Sizer must return worst-case estimate >= actual bytes written", i)
            .isGreaterThanOrEqualTo(actualBytesWritten);
      }
    }
  }

  @Test
  void testComponentSerdeWithNullValues() {
    // Test record with nullable String component
    // Using the existing public TestRecord(String name, int value)

    // Build ComponentSerde array using Companion2
    ComponentSerde[] serdes = Companion2.buildComponentSerdes(
        TestRecord.class,
        List.of(), // No custom handlers for this test
        clazz -> obj -> 0, // Simple sizer resolver
        clazz -> (buffer, obj) -> {
        }, // Simple writer resolver
        clazz -> buffer -> null // Simple reader resolver
    );

    // Test with non-null values
    TestRecord original = new TestRecord("hello", 42);
    ByteBuffer buffer = ByteBuffer.allocate(1024);

    // Calculate size
    int size = 0;
    for (ComponentSerde serde : serdes) {
      size += serde.sizer().applyAsInt(original);
    }
    assertThat(size).isGreaterThan(0);

    // Write components
    serdes[0].writer().accept(buffer, original); // writes name
    serdes[1].writer().accept(buffer, original); // writes value

    // Reset buffer for reading
    buffer.flip();

    // Read components
    String readName = (String) serdes[0].reader().apply(buffer);
    int readValue = (int) serdes[1].reader().apply(buffer);

    assertThat(readName).isEqualTo("hello");
    assertThat(readValue).isEqualTo(42);

    // Test with null String value
    TestRecord withNull = new TestRecord(null, 99);
    buffer.clear();

    // Calculate size with null
    int sizeWithNull = 0;
    for (ComponentSerde serde : serdes) {
      sizeWithNull += serde.sizer().applyAsInt(withNull);
    }
    assertThat(sizeWithNull).isGreaterThan(0);

    // Write components with null
    serdes[0].writer().accept(buffer, withNull); // writes null name
    serdes[1].writer().accept(buffer, withNull); // writes value

    // Reset buffer for reading
    buffer.flip();

    // Read components with null
    String readNullName = (String) serdes[0].reader().apply(buffer);
    int readValueFromNull = (int) serdes[1].reader().apply(buffer);

    assertThat(readNullName).isNull();
    assertThat(readValueFromNull).isEqualTo(99);
  }

  @Test
  void testRuntimeEncodingDecisions() {
    // Test that INTEGER and LONG use runtime encoding decisions

    // Test with small values that should use variable encoding
    ComponentSerde[] smallSerdes = Companion2.buildComponentSerdes(
        SmallValues.class,
        List.of(),
        clazz -> obj -> 0,
        clazz -> (buffer, obj) -> {
        },
        clazz -> buffer -> null
    );

    // Test small values (need values that ZigZag encode to < 128)
    // For ZigZag: positive n becomes 2n, negative -n becomes 2n-1
    // So for 1 byte encoding, we need ZigZag result < 128
    // That means positive values 0-63, negative values -1 to -64
    SmallValues small = new SmallValues(42, 50L);
    ByteBuffer buffer = ByteBuffer.allocate(1024);

    // Write small int (should use INTEGER_VAR marker -7)
    int positionBefore = buffer.position();
    smallSerdes[0].writer().accept(buffer, small);
    int bytesWritten = buffer.position() - positionBefore;

    // For small values, we expect: 1 byte marker + 1 byte value = 2 bytes
    assertThat(bytesWritten).isEqualTo(2);

    // Write small long (should use LONG_VAR marker -9)
    positionBefore = buffer.position();
    final int posBefore = positionBefore;
    LOGGER.fine(() -> "Before writing small long: position=" + posBefore + " value=" + small.smallLong());
    smallSerdes[1].writer().accept(buffer, small);
    bytesWritten = buffer.position() - positionBefore;

    // Debug: log what was actually written
    final int bytesWrittenFinal = bytesWritten;
    LOGGER.fine(() -> {
      StringBuilder sb = new StringBuilder();
      sb.append("Small long value=").append(small.smallLong()).append(" wrote ").append(bytesWrittenFinal).append(" bytes:");
      buffer.position(posBefore);
      for (int i = 0; i < bytesWrittenFinal; i++) {
        sb.append(" byte[").append(i).append("]=").append(buffer.get());
      }
      return sb.toString();
    });

    // For small values, we expect: 1 byte marker + 1 byte value = 2 bytes
    assertThat(bytesWritten).isEqualTo(2);

    // Test with large values that should use fixed encoding
    SmallValues large = new SmallValues(Integer.MAX_VALUE, Long.MAX_VALUE);
    buffer.clear();

    // Write large int (should use INTEGER marker -6)
    positionBefore = buffer.position();
    smallSerdes[0].writer().accept(buffer, large);
    bytesWritten = buffer.position() - positionBefore;

    // For large values, we expect: 1 byte marker + 4 bytes value = 5 bytes
    assertThat(bytesWritten).isEqualTo(5);

    // Write large long (should use LONG marker -8)
    positionBefore = buffer.position();
    smallSerdes[1].writer().accept(buffer, large);
    bytesWritten = buffer.position() - positionBefore;

    // For large values, we expect: 1 byte marker + 8 bytes value = 9 bytes
    assertThat(bytesWritten).isEqualTo(9);

    // Test that the sizer always returns worst-case estimate
    int intSize = smallSerdes[0].sizer().applyAsInt(small);
    int longSize = smallSerdes[1].sizer().applyAsInt(small);

    // Sizer should return worst-case: 5 for int, 9 for long
    assertThat(intSize).isEqualTo(5);
    assertThat(longSize).isEqualTo(9);
  }

  // Test types
  public record TestRecord(String name, int value) {
  }

  public record SmallValues(int smallInt, long smallLong) {
  }

  @SuppressWarnings("unused")
  public enum TestEnum {A, B, C}
}

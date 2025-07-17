// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.BaselineTests;
import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

class TypeExpr2Tests {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  @Test
  void testPrimitiveTypes() {
    LOGGER.info(() -> "Testing primitive types...");
    // Test primitive types get negative markers
    var intExpr = TypeExpr2.analyzeType(int.class, List.of());
    assertThat(intExpr).isInstanceOf(TypeExpr2.PrimitiveValueNode.class);
    var intNode = (TypeExpr2.PrimitiveValueNode) intExpr;
    assertThat(intNode.toTreeString()).isEqualTo("int");

    var boolExpr = TypeExpr2.analyzeType(boolean.class, List.of());
    assertThat(boolExpr).isInstanceOf(TypeExpr2.PrimitiveValueNode.class);
    var boolNode = (TypeExpr2.PrimitiveValueNode) boolExpr;
    assertThat(boolNode.toTreeString()).isEqualTo("boolean");
  }

  @Test
  void testAllPrimitiveAndBoxedTypes() {
    Map<Class<?>, TypeExpr2.PrimitiveValueType> primitiveMap = Map.of(
        byte.class, TypeExpr2.PrimitiveValueType.BYTE,
        short.class, TypeExpr2.PrimitiveValueType.SHORT,
        char.class, TypeExpr2.PrimitiveValueType.CHARACTER,
        long.class, TypeExpr2.PrimitiveValueType.LONG,
        float.class, TypeExpr2.PrimitiveValueType.FLOAT,
        double.class, TypeExpr2.PrimitiveValueType.DOUBLE
    );

    for (var entry : primitiveMap.entrySet()) {
      var expr = TypeExpr2.analyzeType(entry.getKey(), List.of());
      assertThat(expr).isInstanceOf(TypeExpr2.PrimitiveValueNode.class);
      var node = (TypeExpr2.PrimitiveValueNode) expr;
      assertThat(node.type()).isEqualTo(entry.getValue());
      assertThat(node.toTreeString()).isEqualTo(entry.getKey().getSimpleName());
    }

    Map<Class<?>, TypeExpr2.RefValueType> boxedMap = Map.of(
        Byte.class, TypeExpr2.RefValueType.BYTE,
        Short.class, TypeExpr2.RefValueType.SHORT,
        Character.class, TypeExpr2.RefValueType.CHARACTER,
        Long.class, TypeExpr2.RefValueType.LONG,
        Float.class, TypeExpr2.RefValueType.FLOAT,
        Double.class, TypeExpr2.RefValueType.DOUBLE
    );

    for (var entry : boxedMap.entrySet()) {
      var expr = TypeExpr2.analyzeType(entry.getKey(), List.of());
      assertThat(expr).isInstanceOf(TypeExpr2.RefValueNode.class);
      var node = (TypeExpr2.RefValueNode) expr;
      assertThat(node.type()).isEqualTo(entry.getValue());
      assertThat(node.toTreeString()).isEqualTo(entry.getKey().getSimpleName());
    }
  }

  @Test
  void testDateTimeTypes() {
    var localDateExpr = TypeExpr2.analyzeType(LocalDate.class, List.of());
    assertThat(localDateExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var localDateNode = (TypeExpr2.RefValueNode) localDateExpr;
    assertThat(localDateNode.type()).isEqualTo(TypeExpr2.RefValueType.LOCAL_DATE);
    assertThat(localDateNode.toTreeString()).isEqualTo("LocalDate");

    var localDateTimeExpr = TypeExpr2.analyzeType(LocalDateTime.class, List.of());
    assertThat(localDateTimeExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var localDateTimeNode = (TypeExpr2.RefValueNode) localDateTimeExpr;
    assertThat(localDateTimeNode.type()).isEqualTo(TypeExpr2.RefValueType.LOCAL_DATE_TIME);
    assertThat(localDateTimeNode.toTreeString()).isEqualTo("LocalDateTime");
  }

  @Test
  void testBoxedPrimitives() {
    // Test boxed primitives get negative markers
    var integerExpr = TypeExpr2.analyzeType(Integer.class, List.of());
    assertThat(integerExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var integerNode = (TypeExpr2.RefValueNode) integerExpr;
    assertThat(integerNode.toTreeString()).isEqualTo("Integer");
  }

  @Test
  void testBuiltInValueTypes() {
    // Test String gets negative marker
    var stringExpr = TypeExpr2.analyzeType(String.class, List.of());
    assertThat(stringExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var stringNode = (TypeExpr2.RefValueNode) stringExpr;
    assertThat(stringNode.toTreeString()).isEqualTo("String");
  }

  @Test
  void testUserTypes() {
    // Test record gets marker 0 (uses type signature)
    var recordExpr = TypeExpr2.analyzeType(TestRecord.class, List.of());
    assertThat(recordExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var recordNode = (TypeExpr2.RefValueNode) recordExpr;
    assertThat(recordNode.type()).isEqualTo(TypeExpr2.RefValueType.RECORD);
    assertThat(recordNode.toTreeString()).isEqualTo("TestRecord");

    // Test enum gets marker 0 (uses type signature)
    var enumExpr = TypeExpr2.analyzeType(TestEnum.class, List.of());
    assertThat(enumExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var enumNode = (TypeExpr2.RefValueNode) enumExpr;
    assertThat(enumNode.type()).isEqualTo(TypeExpr2.RefValueType.ENUM);
    assertThat(enumNode.toTreeString()).isEqualTo("TestEnum");
  }

  @Test
  void testArrayTypes() {
    // Test primitive array
    var intArrayExpr = TypeExpr2.analyzeType(int[].class, List.of());
    assertThat(intArrayExpr).isInstanceOf(TypeExpr2.PrimitiveArrayNode.class);
    var intArrayNode = (TypeExpr2.PrimitiveArrayNode) intArrayExpr;
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
    assertThat(mapExpr.toTreeString()).isEqualTo("MAP(String,UUID)");
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
  void testOptionalStringRoundTrip() {
    // Test Optional<String> serialization round-trip  
    // Record must be public for reflection access

    // Build ComponentSerde array
    ComponentSerde[] serdes = Companion.buildComponentSerdes(
        OptionalStringRecord.class,
        List.of(), // No custom handlers
        clazz -> obj -> 100, // Simple sizer resolver
        clazz -> (buffer, obj) -> {
        }, // Simple writer resolver
        clazz -> buffer -> null // Simple reader resolver
    );

    ByteBuffer buffer = ByteBuffer.allocate(1024);

    // Test Optional.empty()
    OptionalStringRecord emptyRecord = new OptionalStringRecord(Optional.empty());
    serdes[0].writer().accept(buffer, emptyRecord);
    buffer.flip();

    // Read back and verify
    @SuppressWarnings("unchecked") Optional<String> readEmpty = (Optional<String>) serdes[0].reader().apply(buffer);
    assertThat(readEmpty).isEmpty();

    // Test Optional.of(value)
    buffer.clear();
    OptionalStringRecord valueRecord = new OptionalStringRecord(Optional.of("hello"));
    serdes[0].writer().accept(buffer, valueRecord);
    buffer.flip();

    // Read back and verify
    @SuppressWarnings("unchecked") Optional<String> readValue = (Optional<String>) serdes[0].reader().apply(buffer);
    assertThat(readValue).isPresent();
    assertThat(readValue.get()).isEqualTo("hello");
  }

  public record OptionalIntRecord(Optional<Integer> optInt) {
  }

  @Test
  void testOptionalIntegerRoundTrip() {
    // Test Optional<Integer> serialization round-trip

    ComponentSerde[] serdes = Companion.buildComponentSerdes(
        OptionalIntRecord.class,
        List.of(),
        clazz -> obj -> 100,
        clazz -> (buffer, obj) -> {
        },
        clazz -> buffer -> null
    );

    ByteBuffer buffer = ByteBuffer.allocate(1024);

    // Test Optional.empty()
    OptionalIntRecord emptyRecord = new OptionalIntRecord(Optional.empty());
    serdes[0].writer().accept(buffer, emptyRecord);
    buffer.flip();

    @SuppressWarnings("unchecked") Optional<Integer> readEmpty = (Optional<Integer>) serdes[0].reader().apply(buffer);
    assertThat(readEmpty).isEmpty();

    // Test Optional.of(42)
    buffer.clear();
    OptionalIntRecord valueRecord = new OptionalIntRecord(Optional.of(42));
    serdes[0].writer().accept(buffer, valueRecord);
    buffer.flip();

    @SuppressWarnings("unchecked") Optional<Integer> readValue = (Optional<Integer>) serdes[0].reader().apply(buffer);
    assertThat(readValue).isPresent();
    assertThat(readValue.get()).isEqualTo(42);
  }

  @Test
  void testOptionalRecordRoundTrip() {
    final List<Class<?>> recordTypes = List.of(TestRecord.class);
    final var recordTypeSignatureMap = Companion.computeRecordTypeSignatures(recordTypes);

    ComponentSerde[] serdes = Companion.buildComponentSerdes(
        OptionalRecordHolder.class,
        List.of(),
        // Sizer resolver for user types
        type -> (obj) -> {
          if (type == TestRecord.class) {
            return Long.BYTES + 50; // signature + estimated component size
          }
          return 100;
        },
        // Writer resolver for user types
        type -> (buffer, obj) -> {
          if (type == TestRecord.class) {
            TestRecord record = (TestRecord) obj;
            long sig = recordTypeSignatureMap.get(TestRecord.class);
            buffer.putLong(sig);
            // Write components manually for test
            byte[] nameBytes = record.name() != null ? record.name().getBytes(java.nio.charset.StandardCharsets.UTF_8) : new byte[0];
            ZigZagEncoding.putInt(buffer, nameBytes.length);
            buffer.put(nameBytes);
            buffer.putInt(record.value());
          }
        },
        // Reader resolver for user types
        signature -> buffer -> {
          if (signature.equals(recordTypeSignatureMap.get(TestRecord.class))) {
            int nameLength = ZigZagEncoding.getInt(buffer);
            byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            String name = nameLength > 0 ? new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8) : null;
            int value = buffer.getInt();
            return new TestRecord(name, value);
          }
          return null;
        }
    );

    ByteBuffer buffer = ByteBuffer.allocate(1024);

    // Test Optional.empty()
    OptionalRecordHolder emptyRecord = new OptionalRecordHolder(Optional.empty());
    serdes[0].writer().accept(buffer, emptyRecord);
    buffer.flip();

    @SuppressWarnings("unchecked") Optional<TestRecord> readEmpty = (Optional<TestRecord>) serdes[0].reader().apply(buffer);
    assertThat(readEmpty).isEmpty();

    // Test Optional.of(record)
    buffer.clear();
    TestRecord testRecord = new TestRecord("test", 123);
    OptionalRecordHolder valueRecord = new OptionalRecordHolder(Optional.of(testRecord));
    serdes[0].writer().accept(buffer, valueRecord);
    buffer.flip();

    @SuppressWarnings("unchecked") Optional<TestRecord> readValue = (Optional<TestRecord>) serdes[0].reader().apply(buffer);
    assertThat(readValue).isPresent();
    assertThat(readValue.get()).isEqualTo(testRecord);
  }

  @Test
  void testOptionalMarkers() {
    ComponentSerde[] serdes = Companion.buildComponentSerdes(
        OptionalHolder.class,
        List.of(),
        clazz -> obj -> 100,
        clazz -> (buffer, obj) -> {
        },
        clazz -> buffer -> null
    );

    ByteBuffer buffer = ByteBuffer.allocate(1024);

    // Test Optional.empty() writes OPTIONAL_EMPTY marker
    OptionalHolder emptyRecord = new OptionalHolder(Optional.empty());
    serdes[0].writer().accept(buffer, emptyRecord);

    buffer.flip();
    int emptyMarker = ZigZagEncoding.getInt(buffer);
    assertThat(emptyMarker).isEqualTo(Companion.ContainerType.OPTIONAL_EMPTY.marker());
    assertThat(emptyMarker).isEqualTo(-16);

    // Test Optional.of(value) writes OPTIONAL_OF marker
    buffer.clear();
    OptionalHolder valueRecord = new OptionalHolder(Optional.of("test"));
    serdes[0].writer().accept(buffer, valueRecord);

    buffer.flip();
    int valueMarker = ZigZagEncoding.getInt(buffer);
    assertThat(valueMarker).isEqualTo(Companion.ContainerType.OPTIONAL_OF.marker());
    assertThat(valueMarker).isEqualTo(-17);
  }

  @Test
  void testComponentSerdeBuilding() {
    // Test record with String and int components - must be public for reflection
    // Using the existing public TestRecord(String name, int value)

    // Build ComponentSerde array using Companion
    ComponentSerde[] serdes = Companion.buildComponentSerdes(
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

    // Build ComponentSerde array using Companion
    ComponentSerde[] serdes = Companion.buildComponentSerdes(
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

    // Build ComponentSerde array using Companion
    ComponentSerde[] serdes = Companion.buildComponentSerdes(
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
    ComponentSerde[] smallSerdes = Companion.buildComponentSerdes(
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

  @Test
  void testRecursiveRecordAST() {
    // Analyze the 'value' component
    var valueComp = LinkedListNode.class.getRecordComponents()[0];
    var valueExpr = TypeExpr2.analyzeType(valueComp.getGenericType(), List.of());
    assertThat(valueExpr).isInstanceOf(TypeExpr2.PrimitiveValueNode.class);
    assertThat(valueExpr.toTreeString()).isEqualTo("int");

    // Analyze the 'next' component
    var nextComp = LinkedListNode.class.getRecordComponents()[1];
    var nextExpr = TypeExpr2.analyzeType(nextComp.getGenericType(), List.of());
    assertThat(nextExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var nextNode = (TypeExpr2.RefValueNode) nextExpr;
    assertThat(nextNode.type()).isEqualTo(TypeExpr2.RefValueType.RECORD);
    assertThat(nextNode.javaType()).isEqualTo(LinkedListNode.class);
    assertThat(nextNode.toTreeString()).isEqualTo("LinkedListNode");
  }

  @Test
  @DisplayName("Test recursive record (LinkedListNode) round trips")
  void testRecursiveRecordRoundTrip() {
    // 1. Create a "mini-pickler" simulation for LinkedListNode
    final var recordClass = LinkedListNode.class;
    final List<Class<?>> userTypes = List.of(recordClass);
    final var recordClassToTypeSignatureMap = Companion.computeRecordTypeSignatures(userTypes);
    final var typeSignature = recordClassToTypeSignatureMap.get(recordClass);
    LOGGER.info(() -> "Test: LinkedListNode typeSignature = " + Long.toHexString(typeSignature));

    // 2. Build the ComponentSerdes using Companion
    final ComponentSerde[][] serdes = {null};
    serdes[0] = Companion.buildComponentSerdes(
        recordClass,
        List.of(), // No custom handlers
        // Sizer Resolver: Delegates back to the main sizer
        type -> (obj) -> {
          if (obj == null) return Byte.BYTES; // Null marker
          // For a LinkedListNode, it's: typeSignature + component data
          return Long.BYTES + serdes[0][0].sizer().applyAsInt(obj) + serdes[0][1].sizer().applyAsInt(obj);
        },
        // Writer Resolver: Delegates back to the main writer
        type -> (buffer, obj) -> {
          if (obj == null) {
            LOGGER.fine("WriterResolver: Writing null signature (0L)");
            buffer.putLong(0L); // Special null signature
            return;
          }
          // LOG THE HASHCODE AS REQUESTED
          LOGGER.info(() -> "WriterResolver: Writing object with hashCode: " + obj.hashCode());
          long sig = recordClassToTypeSignatureMap.get(obj.getClass());
          LOGGER.fine("WriterResolver: Writing signature " + Long.toHexString(sig) + " for object: " + obj);
          buffer.putLong(sig);
          serdes[0][0].writer().accept(buffer, obj);
          serdes[0][1].writer().accept(buffer, obj);
        },
        // Reader Resolver
        type -> buffer -> {
          var val = (int) serdes[0][0].reader().apply(buffer);
          var next = (LinkedListNode) serdes[0][1].reader().apply(buffer);
          return new LinkedListNode(val, next);
        }
    );

    // Create an instance and log the object hashCodes so that we can verify they are written to the wire logged by the writer
    LinkedListNode original = new LinkedListNode(1, new LinkedListNode(2, new LinkedListNode(3)));
    LOGGER.info(() -> "Test: original.hashCode() = " + original.hashCode());
    LOGGER.info(() -> "Test: original.next.hashCode() = " + original.next().hashCode());
    LOGGER.info(() -> "Test: original.next.next.hashCode() = " + original.next().next().hashCode());


    // Manually serialize
    ByteBuffer buffer = ByteBuffer.allocate(1024);

    // Get the writer for the LinkedListNode type from the resolver
    var writerResolver = (Function<Class<?>, BiConsumer<ByteBuffer, Object>>) type -> (BiConsumer<ByteBuffer, Object>) (buf, obj) -> {
      if (obj == null) {
        LOGGER.fine("WriterResolver: Writing null signature (0L)");
        buf.putLong(0L); // Special null signature
        return;
      }
      // LOG THE HASHCODE AS REQUESTED
      LOGGER.info(() -> "WriterResolver: Writing object with hashCode: " + obj.hashCode());
      long sig = recordClassToTypeSignatureMap.get(obj.getClass());
      LOGGER.fine("WriterResolver: Writing signature " + Long.toHexString(sig) + " for object: " + obj);
      buf.putLong(sig);
      serdes[0][0].writer().accept(buf, obj);
      serdes[0][1].writer().accept(buf, obj);
    };

    var nodeWriter = writerResolver.apply(recordClass);
    nodeWriter.accept(buffer, original);

    buffer.flip();

    // Manually deserialize
    var readerResolver = (Function<Long, Function<ByteBuffer, Object>>) type -> (Function<ByteBuffer, Object>) buf -> {
      var val = (int) serdes[0][0].reader().apply(buf);
      var next = (LinkedListNode) serdes[0][1].reader().apply(buf);
      return new LinkedListNode(val, next);
    };

    long signature = buffer.getLong();
    var objectReader = readerResolver.apply(signature);
    LinkedListNode deserialized = (LinkedListNode) objectReader.apply(buffer);

    assertThat(deserialized).isEqualTo(original);
  }

  @Test
  void testSealedInterfaceAST() {
    // Analyze the 'next' component of the LinkedRecord
    var nextComp = LinkListEmptyEnd.LinkedRecord.class.getRecordComponents()[1];
    var nextExpr = TypeExpr2.analyzeType(nextComp.getGenericType(), List.of());
    assertThat(nextExpr).isInstanceOf(TypeExpr2.RefValueNode.class);
    var nextNode = (TypeExpr2.RefValueNode) nextExpr;
    assertThat(nextNode.type()).isEqualTo(TypeExpr2.RefValueType.INTERFACE);
    assertThat(nextNode.javaType()).isEqualTo(LinkListEmptyEnd.class);
    assertThat(nextNode.toTreeString()).isEqualTo("LinkListEmptyEnd");
  }

  @Test
  @DisplayName("Test sealed interface (LinkListEmptyEnd) round trips")
  void testSealedInterfaceRoundTrip() {
    // 1. Define user types (both records and the sealed interface)
    final List<Class<?>> recordTypes = List.of(
        LinkListEmptyEnd.LinkedRecord.class,
        LinkListEmptyEnd.LinkEnd.class,
        LinkListEmptyEnd.Boxed.class
    );
    final var recordTypeSignatureMap = Companion.computeRecordTypeSignatures(recordTypes);

    // Log the signatures for debugging
    recordTypeSignatureMap.forEach((type, sig) ->
        LOGGER.info(() -> "Test: " + type.getSimpleName() + " typeSignature = " + Long.toHexString(sig)));

    // 2. Build the ComponentSerdes for the top-level LinkedRecord
    final ComponentSerde[][] serdes = {null}; // Array wrapper for closure
    serdes[0] = Companion.buildComponentSerdes(
        LinkListEmptyEnd.LinkedRecord.class,
        List.of(), // No custom handlers
        // Sizer Resolver: Handles all user types
        type -> (obj) -> {
          if (obj == null) return Long.BYTES; // Null signature
          if (type == LinkListEmptyEnd.Boxed.class) {
            // Boxed record: signature + int component
            return Long.BYTES + Integer.BYTES;
          } else if (type == LinkListEmptyEnd.LinkedRecord.class) {
            // LinkedRecord: signature + component sizes
            return Long.BYTES + serdes[0][0].sizer().applyAsInt(obj) + serdes[0][1].sizer().applyAsInt(obj);
          } else if (type == LinkListEmptyEnd.LinkEnd.class) {
            // LinkEnd (empty record): just signature
            return Long.BYTES;
          } else {
            throw new IllegalArgumentException("Unknown type: " + type);
          }
        },
        // Writer Resolver: Handles all user types
        type -> (buffer, obj) -> {
          if (obj == null) {
            LOGGER.fine("WriterResolver: Writing null signature (0L)");
            buffer.putLong(0L);
            return;
          }

          LOGGER.info(() -> "WriterResolver: Writing object with hashCode: " + obj.hashCode() + " type: " + obj.getClass().getSimpleName());
          long sig = recordTypeSignatureMap.get(obj.getClass());
          LOGGER.fine("WriterResolver: Writing signature " + Long.toHexString(sig) + " for object: " + obj);
          buffer.putLong(sig);

          switch (obj) {
            case LinkListEmptyEnd.Boxed(int value) -> buffer.putInt(value);
            case LinkListEmptyEnd.LinkedRecord linked -> {
              serdes[0][0].writer().accept(buffer, linked);
              serdes[0][1].writer().accept(buffer, linked);
            }
            case LinkListEmptyEnd.LinkEnd ignored -> // LinkEnd is empty - nothing to write after signature
                LOGGER.fine(() -> "WriterResolver: Writing LinkEnd, nothing to write after signature");
            default -> throw new IllegalArgumentException("Unknown object type: " + obj.getClass());
          }
        },
        // Reader Resolver: Handles all user types by signature
        signature -> buffer -> {
          if (signature == 0L) {
            return null; // Null object
          }

          // Find the type by signature
          Class<?> targetType = recordTypeSignatureMap.entrySet().stream()
              .filter(entry -> entry.getValue().equals(signature))
              .map(Map.Entry::getKey)
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("Unknown signature: " + Long.toHexString(signature)));

          LOGGER.fine("ReaderResolver: Reading type " + targetType.getSimpleName() + " from signature " + Long.toHexString(signature));

          if (targetType == LinkListEmptyEnd.Boxed.class) {
            int value = buffer.getInt();
            return new LinkListEmptyEnd.Boxed(value);
          } else if (targetType == LinkListEmptyEnd.LinkedRecord.class) {
            var value = (LinkListEmptyEnd.Boxed) serdes[0][0].reader().apply(buffer);
            var next = (LinkListEmptyEnd) serdes[0][1].reader().apply(buffer);
            return new LinkListEmptyEnd.LinkedRecord(value, next);
          } else if (targetType == LinkListEmptyEnd.LinkEnd.class) {
            return new LinkListEmptyEnd.LinkEnd();
          } else {
            throw new IllegalArgumentException("Unknown target type: " + targetType);
          }
        }
    );

    // 3. Create test data with sealed interface hierarchy
    LinkListEmptyEnd.Boxed value1 = new LinkListEmptyEnd.Boxed(42);
    LinkListEmptyEnd.Boxed value2 = new LinkListEmptyEnd.Boxed(99);
    LinkListEmptyEnd.LinkEnd end = new LinkListEmptyEnd.LinkEnd();
    LinkListEmptyEnd.LinkedRecord middle = new LinkListEmptyEnd.LinkedRecord(value2, end);
    LinkListEmptyEnd.LinkedRecord original = new LinkListEmptyEnd.LinkedRecord(value1, middle);

    LOGGER.info(() -> "Test: original structure created with hashCodes - " +
        "original: " + original.hashCode() +
        ", value1: " + value1.hashCode() +
        ", middle: " + middle.hashCode() +
        ", value2: " + value2.hashCode() +
        ", end: " + end.hashCode());

    // 4. Manually serialize using the writer resolver
    ByteBuffer buffer = ByteBuffer.allocate(1024);

    var writerResolver = (Function<Class<?>, BiConsumer<ByteBuffer, Object>>) type -> (buffer1, obj) -> {
      if (obj == null) {
        LOGGER.fine("WriterResolver: Writing null signature (0L)");
        buffer1.putLong(0L);
        return;
      }

      LOGGER.info(() -> "WriterResolver: Writing object with hashCode: " + obj.hashCode() + " type: " + obj.getClass().getSimpleName());
      long sig = recordTypeSignatureMap.get(obj.getClass());
      LOGGER.fine("WriterResolver: Writing signature " + Long.toHexString(sig) + " for object: " + obj);
      buffer1.putLong(sig);

      switch (obj) {
        case LinkListEmptyEnd.Boxed(int value) -> buffer1.putInt(value);
        case LinkListEmptyEnd.LinkedRecord linked -> {
          serdes[0][0].writer().accept(buffer1, linked);
          serdes[0][1].writer().accept(buffer1, linked);
        }
        case LinkListEmptyEnd.LinkEnd ignored ->
          // LinkEnd is empty - nothing to write after signature
            LOGGER.fine(() -> "WriterResolver: Writing LinkEnd, nothing to write after signature");
        default -> throw new IllegalArgumentException("Unknown object type: " + obj.getClass());
      }
    };

    var recordWriter = writerResolver.apply(LinkListEmptyEnd.LinkedRecord.class);
    recordWriter.accept(buffer, original);

    buffer.flip();

    // 5. Manually deserialize using the reader resolver
    var readerResolver = (Function<Long, Function<ByteBuffer, Object>>) signature -> buffer1 -> {
      if (signature == 0L) {
        return null; // Null object
      }

      // Find the type by signature
      Class<?> targetType = recordTypeSignatureMap.entrySet().stream()
          .filter(entry -> entry.getValue().equals(signature))
          .map(Map.Entry::getKey)
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Unknown signature: " + Long.toHexString(signature)));

      LOGGER.fine("ReaderResolver: Reading type " + targetType.getSimpleName() + " from signature " + Long.toHexString(signature));

      if (targetType == LinkListEmptyEnd.Boxed.class) {
        int value = buffer.getInt();
        return new LinkListEmptyEnd.Boxed(value);
      } else if (targetType == LinkListEmptyEnd.LinkedRecord.class) {
        var value = (LinkListEmptyEnd.Boxed) serdes[0][0].reader().apply(buffer);
        var next = (LinkListEmptyEnd) serdes[0][1].reader().apply(buffer);
        return new LinkListEmptyEnd.LinkedRecord(value, next);
      } else if (targetType == LinkListEmptyEnd.LinkEnd.class) {
        return new LinkListEmptyEnd.LinkEnd();
      } else {
        throw new IllegalArgumentException("Unknown target type: " + targetType);
      }
    };

    long signature = buffer.getLong();
    var objectReader = readerResolver.apply(signature);
    LinkListEmptyEnd.LinkedRecord deserialized = (LinkListEmptyEnd.LinkedRecord) objectReader.apply(buffer);

    // 6. Verify the round trip worked
    assertThat(deserialized).isEqualTo(original);
    assertThat(deserialized.value()).isEqualTo(original.value());
    assertThat(deserialized.next()).isInstanceOf(LinkListEmptyEnd.LinkedRecord.class);

    LinkListEmptyEnd.LinkedRecord deserializedMiddle = (LinkListEmptyEnd.LinkedRecord) deserialized.next();
    assertThat(deserializedMiddle.value()).isEqualTo(value2);
    assertThat(deserializedMiddle.next()).isInstanceOf(LinkListEmptyEnd.LinkEnd.class);
  }

  // Test types
  public record TestRecord(String name, int value) {
  }

  public record SmallValues(int smallInt, long smallLong) {
  }

  public record OptionalStringRecord(Optional<String> optString) {
  }

  public record OptionalRecordHolder(Optional<TestRecord> optRecord) {
  }

  public record OptionalHolder(Optional<String> value) {
  }

  @SuppressWarnings("unused")
  public enum TestEnum {A, B, SECOND, THIRD}

  public record LinkedListNode(int value, LinkedListNode next) {
    public LinkedListNode(int value) {
      this(value, null);
    }
  }

  public record ListStringRecord(List<String> list) {
  }

  @Test
  void testListStringRoundTrip() {
    // Test List<String> serialization round-trip
    ComponentSerde[] serdes = Companion.buildComponentSerdes(
        ListStringRecord.class,
        List.of(), // No custom handlers
        clazz -> obj -> 100, // Simple sizer resolver
        clazz -> (buffer, obj) -> {
        }, // Simple writer resolver
        clazz -> buffer -> null // Simple reader resolver
    );

    ByteBuffer buffer = ByteBuffer.allocate(1024);

    // Test with a list of strings
    ListStringRecord record = new ListStringRecord(java.util.Arrays.asList("hello", "world", null, "again"));
    serdes[0].writer().accept(buffer, record);
    buffer.flip();

    // Read back and verify
    @SuppressWarnings("unchecked")
    List<String> readList = (List<String>) serdes[0].reader().apply(buffer);
    assertThat(readList).containsExactly("hello", "world", null, "again");

    // Test with a null list
    buffer.clear();
    ListStringRecord nullListRecord = new ListStringRecord(null);
    serdes[0].writer().accept(buffer, nullListRecord);
    buffer.flip();

    @SuppressWarnings("unchecked")
    List<String> readNullList = (List<String>) serdes[0].reader().apply(buffer);
    assertThat(readNullList).isNull();
  }

  // inner interface of nested records and empty record end
  public sealed interface LinkListEmptyEnd permits LinkListEmptyEnd.LinkedRecord, LinkListEmptyEnd.LinkEnd {

    // record implementing interface
    record LinkedRecord(Boxed value, LinkListEmptyEnd next) implements LinkListEmptyEnd {
    }

    // enum implementing interface
    record LinkEnd() implements LinkListEmptyEnd {
    }

    // inner record
    record Boxed(int value) {
    }
  }

  public sealed interface HeterogeneousItem permits ItemList, ItemOptional, ItemString, ItemInt, ItemLong, ItemBoolean, ItemNull, ItemTestRecord, ItemTestEnum {
  }

  public record ItemString(String value) implements HeterogeneousItem {
  }

  public record ItemInt(Integer value) implements HeterogeneousItem {
  }

  public record ItemLong(Long value) implements HeterogeneousItem {
  }

  public record ItemBoolean(Boolean value) implements HeterogeneousItem {
  }

  public record ItemNull() implements HeterogeneousItem {
  }

  public record ItemTestRecord(BaselineTests.Person value) implements HeterogeneousItem {
  }

  public record ItemTestEnum(TestEnum value) implements HeterogeneousItem {
  }

  public record ItemOptional(Optional<String> value) implements HeterogeneousItem {
  }

  public record ItemList(List<String> value) implements HeterogeneousItem {
  }

  public record ComplexListRecord(List<HeterogeneousItem> list) {
  }

  @Test
  void debugTestComplexHeterogeneousListFirstThreeItems() {
    // Create test data with first three elements only
    List<HeterogeneousItem> simpleList = List.of(
        new ItemString("a string"),
        new ItemInt(123),
        new ItemLong(456L)
    );
    ComplexListRecord originalRecord = new ComplexListRecord(simpleList);

    // Same resolver setup as main test
    final List<Class<?>> sealedTypes = List.of(
        ItemString.class, ItemInt.class, ItemLong.class, ItemBoolean.class, ItemNull.class,
        ItemTestRecord.class, ItemTestEnum.class, ItemOptional.class, ItemList.class
    );
    final List<Class<?>> allRecordTypes = new ArrayList<>(sealedTypes);
    allRecordTypes.add(BaselineTests.Person.class);
    allRecordTypes.add(ComplexListRecord.class);
    final var recordTypeSignatureMap = Companion.computeRecordTypeSignatures(allRecordTypes);
    final long testEnumSignature = Companion.hashEnumSignature(TestEnum.class);
    final Map<Class<?>, ComponentSerde[]> serdeMap = new HashMap<>();

    ComponentSerde[] serdes = Companion.buildComponentSerdes(
        ComplexListRecord.class,
        List.of(),
        type -> (obj) -> 256,
        type -> (buffer, obj) -> {
          if (type == HeterogeneousItem.class) {
            Class<?> concreteType = obj.getClass();
            LOGGER.fine(() -> "WriterResolver: HeterogeneousItem interface, delegating to concrete type: " + concreteType.getSimpleName());
            long sig = recordTypeSignatureMap.get(concreteType);
            LOGGER.fine(() -> "WriterResolver: Writing " + concreteType.getSimpleName() + " signature " + Long.toHexString(sig));
            buffer.putLong(sig);
            var componentSerdes = serdeMap.computeIfAbsent(concreteType, t ->
                Companion.buildComponentSerdes(t, List.of(),
                    type2 -> (o) -> 256,
                    type2 -> (buf, o) -> {
                    },
                    sig2 -> buf -> null));
            if (componentSerdes.length > 0) {
              componentSerdes[0].writer().accept(buffer, obj);
            }
            return;
          }
          throw new IllegalArgumentException("Unknown type for writer: " + type);
        },
        signature -> buffer -> {
          Class<?> targetType = recordTypeSignatureMap.entrySet().stream()
              .filter(entry -> entry.getValue().equals(signature))
              .map(Map.Entry::getKey)
              .findFirst()
              .orElse(null);
          if (targetType != null && sealedTypes.contains(targetType)) {
            var componentSerdes = serdeMap.computeIfAbsent(targetType, t ->
                Companion.buildComponentSerdes(t, List.of(),
                    type -> (obj) -> 256,
                    type2 -> (buf, o) -> {
                    },
                    sig2 -> buf -> null));

            // Correctly handle records with no components (like ItemNull)
            if (componentSerdes.length == 0) {
              if (targetType == ItemNull.class) {
                return new ItemNull();
              }
              // In a real scenario, you might have a factory or reflection-based instantiation
              throw new IllegalStateException("Cannot create instance of zero-component record: " + targetType.getName());
            }

            Object componentValue = componentSerdes[0].reader().apply(buffer);
            if (targetType == ItemString.class) {
              return new ItemString((String) componentValue);
            } else if (targetType == ItemInt.class) {
              return new ItemInt((Integer) componentValue);
            } else if (targetType == ItemLong.class) {
              return new ItemLong((Long) componentValue);
            }
          }
          throw new IllegalArgumentException("Unknown signature: " + Long.toHexString(signature));
        }
    );

    serdeMap.put(ComplexListRecord.class, serdes);

    ByteBuffer buffer = ByteBuffer.allocate(4096);
    serdes[0].writer().accept(buffer, originalRecord);
    buffer.flip();

    @SuppressWarnings("unchecked")
    List<HeterogeneousItem> deserializedList = (List<HeterogeneousItem>) serdes[0].reader().apply(buffer);

    assertThat(deserializedList).hasSize(3);
    assertThat(deserializedList.get(0)).isEqualTo(simpleList.get(0));
    assertThat(deserializedList.get(1)).isEqualTo(simpleList.get(1));
    assertThat(deserializedList.get(2)).isEqualTo(simpleList.get(2));
  }

  @Test
  void debugTestComplexHeterogeneousListSingleItem() {
    // Create test data with just one element
    List<HeterogeneousItem> simpleList = List.of(new ItemString("test"));
    ComplexListRecord originalRecord = new ComplexListRecord(simpleList);

    // Component serdes with focused debugging
    ComponentSerde[] serdes = Companion.buildComponentSerdes(
        ComplexListRecord.class,
        List.of(),
        type -> (obj) -> 256,
        type -> (buffer, obj) -> {
          if (type == HeterogeneousItem.class) {
            Class<?> concreteType = obj.getClass();
            LOGGER.fine(() -> "WriterResolver: HeterogeneousItem interface, delegating to concrete type: " + concreteType.getSimpleName());
            long sig = 1L; // Fixed signature for testing
            LOGGER.fine(() -> "WriterResolver: Writing " + concreteType.getSimpleName() + " signature " + Long.toHexString(sig));
            buffer.putLong(sig);
            if (obj instanceof ItemString str) {
              LOGGER.fine(() -> "WriterResolver: Writing ItemString value: " + str.value());
              ZigZagEncoding.putInt(buffer, Companion.referenceToMarker(String.class));
              byte[] bytes = str.value().getBytes(java.nio.charset.StandardCharsets.UTF_8);
              ZigZagEncoding.putInt(buffer, bytes.length);
              buffer.put(bytes);
            }
            return;
          }
          throw new IllegalArgumentException("Unknown type for writer: " + type);
        },
        signature -> buffer -> {
          if (signature == 1L) {
            LOGGER.fine(() -> "ReaderResolver: Reading ItemString from signature " + Long.toHexString(signature));
            int marker = ZigZagEncoding.getInt(buffer);
            if (marker != Companion.referenceToMarker(String.class)) {
              throw new IllegalStateException("Expected STRING marker");
            }
            int length = ZigZagEncoding.getInt(buffer);
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            String value = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            return new ItemString(value);
          }
          throw new IllegalArgumentException("Unknown signature: " + Long.toHexString(signature));
        }
    );

    ByteBuffer buffer = ByteBuffer.allocate(4096);

    // Serialize
    serdes[0].writer().accept(buffer, originalRecord);
    buffer.flip();

    // Deserialize
    @SuppressWarnings("unchecked")
    List<HeterogeneousItem> deserializedList = (List<HeterogeneousItem>) serdes[0].reader().apply(buffer);

    // Assert
    assertThat(deserializedList).hasSize(1);
    assertThat(deserializedList.get(0)).isEqualTo(simpleList.get(0));
  }
}

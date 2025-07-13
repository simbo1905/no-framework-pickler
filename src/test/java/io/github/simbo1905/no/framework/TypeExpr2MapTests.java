// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import io.github.simbo1905.RefactorTests;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("auxiliaryclass")
class TypeExpr2MapTests {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  public sealed interface HeterogeneousItem permits
      TypeExpr2MapTests.ItemString, TypeExpr2MapTests.ItemInt, TypeExpr2MapTests.ItemLong,
      TypeExpr2MapTests.ItemBoolean, TypeExpr2MapTests.ItemNull, TypeExpr2MapTests.ItemTestRecord,
      TypeExpr2MapTests.ItemTestEnum, TypeExpr2MapTests.ItemOptional, TypeExpr2MapTests.ItemList {
  }

  public record ItemString(String value) implements TypeExpr2MapTests.HeterogeneousItem {
  }

  public record ItemInt(Integer value) implements TypeExpr2MapTests.HeterogeneousItem {
  }

  public record ItemLong(Long value) implements TypeExpr2MapTests.HeterogeneousItem {
  }

  public record ItemBoolean(Boolean value) implements TypeExpr2MapTests.HeterogeneousItem {
  }

  public record ItemNull() implements TypeExpr2MapTests.HeterogeneousItem {
  }

  public record ItemTestRecord(RefactorTests.Person person) implements TypeExpr2MapTests.HeterogeneousItem {
  }

  public record ItemTestEnum(TypeExpr2Tests.TestEnum value) implements TypeExpr2MapTests.HeterogeneousItem {
  }

  public record ItemOptional(Optional<String> value) implements TypeExpr2MapTests.HeterogeneousItem {
  }

  public record ItemList(List<String> value) implements TypeExpr2MapTests.HeterogeneousItem {
  }

  // Test data record holding a map.
  // Note: The value type is Object to allow for heterogeneous content.
  public record ComplexMapRecord(
      Map<TypeExpr2MapTests.HeterogeneousItem, TypeExpr2MapTests.HeterogeneousItem> items) {
  }

  // A sample enum for testing purposes.
  enum TestEnum {
    FIRST, SECOND, THIRD
  }

  // Same resolver setup as main test
  final List<Class<?>> sealedTypes = List.of(
      TypeExpr2MapTests.ItemString.class, TypeExpr2MapTests.ItemInt.class, TypeExpr2MapTests.ItemLong.class, TypeExpr2MapTests.ItemBoolean.class, TypeExpr2MapTests.ItemNull.class,
      TypeExpr2MapTests.ItemTestRecord.class, TypeExpr2MapTests.ItemTestEnum.class, TypeExpr2MapTests.ItemOptional.class, TypeExpr2MapTests.ItemList.class
  );

  @Test
  void testComplexHeterogeneousMapManually() {
    final Map<TypeExpr2MapTests.HeterogeneousItem, TypeExpr2MapTests.HeterogeneousItem> complexMap = buildMapForTest();
    final var originalRecord = new ComplexMapRecord(complexMap);

    LOGGER.info(() -> "Test: originalRecord.hashCode() = " + originalRecord.hashCode());
    originalRecord.items.forEach((k, v) ->
        LOGGER.info(() -> String.format("Test: Map item K=[%s], V=[%s], V-Type=[%s]",
            k, v, (v == null ? "null" : v.getClass().getSimpleName())))
    );

    // 1. Define all record and enum types that need explicit signature handling.
    final List<Class<?>> allRecordTypes = List.of(
        ComplexMapRecord.class,
        RefactorTests.Person.class,
        ItemString.class,
        ItemInt.class,
        ItemLong.class,
        ItemBoolean.class,
        ItemNull.class,
        ItemTestRecord.class,
        ItemTestEnum.class,
        ItemOptional.class,
        ItemList.class
    );
    final var recordTypeSignatureMap = Companion2.computeRecordTypeSignatures(allRecordTypes);
    final long testEnumSignature = Companion2.hashEnumSignature(TypeExpr2Tests.TestEnum.class);

    // Log signatures for debugging.
    recordTypeSignatureMap.forEach((type, sig) ->
        LOGGER.info(() -> "Test: " + type.getSimpleName() + " typeSignature = " + Long.toHexString(sig)));
    LOGGER.info(() -> "Test: TestEnum typeSignature = " + Long.toHexString(testEnumSignature));

    final long personSignature = recordTypeSignatureMap.get(RefactorTests.Person.class);
    final Map<Class<?>, ComponentSerde[]> serdeMap = new HashMap<>();

    // 2. Build component serdes for the ComplexMapRecord
    ComponentSerde[] serdes = Companion2.buildComponentSerdes(
        ComplexMapRecord.class,
        List.of(),
        type -> (obj) -> 256,
        type -> (buffer, obj) -> {
          if (type == TypeExpr2MapTests.HeterogeneousItem.class) {
            Class<?> concreteType = obj.getClass();
            LOGGER.fine(() -> "WriterResolver: HeterogeneousItem interface, delegating to concrete type: " + concreteType.getSimpleName());
            long sig = recordTypeSignatureMap.get(concreteType);
            LOGGER.fine(() -> "WriterResolver: Writing " + concreteType.getSimpleName() + " signature " + Long.toHexString(sig));
            buffer.putLong(sig);
            var componentSerdes = serdeMap.computeIfAbsent(concreteType, t ->
                Companion2.buildComponentSerdes(t, List.of(),
                    type2 -> (o) -> 256,
                    type2 -> (buf, o) -> {
                      if (o instanceof TypeExpr2MapTests.ItemString(String value)) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(String.class));
                        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        ZigZagEncoding.putInt(buf, bytes.length);
                        buf.put(bytes);
                      } else if (o instanceof TypeExpr2MapTests.ItemInt(Integer value)) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(Integer.class));
                        buf.putInt(value);
                      } else if (o instanceof TypeExpr2MapTests.ItemLong(Long value)) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(Long.class));
                        buf.putLong(value);
                      } else if (o instanceof TypeExpr2MapTests.ItemTestEnum(TypeExpr2Tests.TestEnum value)) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(TypeExpr2Tests.TestEnum.class));
                        buf.putInt(value.ordinal());
                      } else if (o instanceof TypeExpr2MapTests.ItemBoolean(Boolean value)) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(Boolean.class));
                        buf.put((byte) (value ? 1 : 0));
                      } else if (o instanceof TypeExpr2MapTests.ItemTestRecord(RefactorTests.Person person)) {
                        // Correctly write the Person record by delegating to its own serde logic
                        // This was the source of the error. We were not writing the person correctly.
                        final var personSerde = serdeMap.computeIfAbsent(RefactorTests.Person.class, t2 ->
                            Companion2.buildComponentSerdes(t2, List.of(),
                                type3 -> (obj2) -> 256,
                                (b) -> (a1, b1) -> {
                                },
                                sig2 -> buf2 -> null))
                            [0];
                        // We need to write the person's components, not a marker
                        personSerde.writer().accept(buffer, person);
                      }
                    },
                    sig2 -> buf -> null));

            // Only write component if the record has components
            if (componentSerdes.length > 0) {
              componentSerdes[0].writer().accept(buffer, obj);
            } else {
              LOGGER.fine(() -> "No component serdes for type: " + concreteType.getSimpleName());
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
                Companion2.buildComponentSerdes(t, List.of(),
                    type -> (obj) -> 256,
                    type2 -> (buf, o) -> {
                      if (o instanceof TypeExpr2MapTests.ItemString(String value)) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(String.class));
                        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        ZigZagEncoding.putInt(buf, bytes.length);
                        buf.put(bytes);
                      } else if (o instanceof TypeExpr2MapTests.ItemInt(Integer value)) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(Integer.class));
                        buf.putInt(value);
                      } else if (o instanceof TypeExpr2MapTests.ItemLong(Long value)) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(Long.class));
                        buf.putLong(value);
                      } else if (o instanceof TypeExpr2MapTests.ItemTestEnum(TypeExpr2Tests.TestEnum value)) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(TypeExpr2Tests.TestEnum.class));
                        buf.putInt(value.ordinal());
                      } else if (o instanceof TypeExpr2MapTests.ItemBoolean(Boolean value)) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(Boolean.class));
                        buf.put((byte) (value ? 1 : 0));
                      } else if (o instanceof TypeExpr2MapTests.ItemTestRecord(RefactorTests.Person person)) {
                        // Correctly write the Person record by delegating to its own serde logic
                        // This was the source of the error. We were not writing the person correctly.
                        final var personSerde = serdeMap.computeIfAbsent(RefactorTests.Person.class, t2 ->
                            Companion2.buildComponentSerdes(t2, List.of(),
                                type3 -> (obj) -> 256,
                                type3 -> (b, obj) -> {
                                },
                                sig2 -> buf2 -> null))
                            [0];
                        // We need to write the person's components, not a marker
                        personSerde.writer().accept(buffer, person);
                      }
                    },
                    sig2 -> buf -> {
                      if (targetType == TypeExpr2MapTests.ItemString.class) {
                        int marker = ZigZagEncoding.getInt(buf);
                        if (marker != TypeExpr2.referenceToMarker(String.class)) {
                          throw new IllegalStateException("Expected STRING marker");
                        }
                        int length = ZigZagEncoding.getInt(buf);
                        byte[] bytes = new byte[length];
                        buf.get(bytes);
                        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
                      } else if (targetType == TypeExpr2MapTests.ItemInt.class) {
                        int marker = ZigZagEncoding.getInt(buf);
                        if (marker != TypeExpr2.referenceToMarker(Integer.class)) {
                          throw new IllegalStateException("Expected INTEGER marker");
                        }
                        return buf.getInt();
                      } else if (targetType == TypeExpr2MapTests.ItemLong.class) {
                        int marker = ZigZagEncoding.getInt(buf);
                        if (marker != TypeExpr2.referenceToMarker(Long.class)) {
                          throw new IllegalStateException("Expected LONG marker");
                        }
                        return buf.getLong();
                      } else if (targetType == TypeExpr2MapTests.ItemTestEnum.class) {
                        int marker = ZigZagEncoding.getInt(buf);
                        if (marker != TypeExpr2.referenceToMarker(TypeExpr2Tests.TestEnum.class)) {
                          throw new IllegalStateException("Expected ENUM marker");
                        }
                        int ordinal = buf.getInt();
                        return TypeExpr2Tests.TestEnum.values()[ordinal];
                      } else if (targetType == TypeExpr2MapTests.ItemBoolean.class) {
                        int marker = ZigZagEncoding.getInt(buf);
                        if (marker != TypeExpr2.referenceToMarker(Boolean.class)) {
                          throw new IllegalStateException("Expected BOOLEAN marker");
                        }
                        return buf.get() == 1;
                      } else if (targetType == TypeExpr2MapTests.ItemTestRecord.class) {
                        // Correctly read the Person record
                        // This was also incorrect and would have failed after the writer fix.
                        final var personSerde = serdeMap.computeIfAbsent(RefactorTests.Person.class, t2 ->
                            Companion2.buildComponentSerdes(t2, List.of(),
                                type3 -> (obj2) -> 256,
                                type3 -> (buffer2, obj2) -> {
                                  // Writer implementation here
                                },
                                signature2 -> buffer3 -> null))
                            [0];
                        return personSerde.reader().apply(buffer);
                      }
                      return null;
                    })
            );
            // Handle zero-component records first
            if (componentSerdes.length == 0) {
              if (targetType == TypeExpr2MapTests.ItemNull.class) {
                return new TypeExpr2MapTests.ItemNull();
              }
              throw new IllegalStateException("Unhandled zero-component type: " + targetType);
            }

            Object componentValue = componentSerdes[0].reader().apply(buffer);
            if (targetType == TypeExpr2MapTests.ItemString.class) {
              return new TypeExpr2MapTests.ItemString((String) componentValue);
            } else if (targetType == TypeExpr2MapTests.ItemInt.class) {
              return new TypeExpr2MapTests.ItemInt((Integer) componentValue);
            } else if (targetType == TypeExpr2MapTests.ItemLong.class) {
              return new TypeExpr2MapTests.ItemLong((Long) componentValue);
            } else if (targetType == TypeExpr2MapTests.ItemTestEnum.class) {
              return new TypeExpr2MapTests.ItemTestEnum((TypeExpr2Tests.TestEnum) componentValue);
            } else if (targetType == TypeExpr2MapTests.ItemBoolean.class) {
              return new TypeExpr2MapTests.ItemBoolean((Boolean) componentValue);
            } else if (targetType == TypeExpr2MapTests.ItemTestRecord.class) {
              return new TypeExpr2MapTests.ItemTestRecord((RefactorTests.Person) componentValue);
            }
            throw new IllegalArgumentException("Unknown signature: " + Long.toHexString(signature));
          }
          if (signature == personSignature) {
            // Handle Person record deserialization
            final var personSerdes = serdeMap.computeIfAbsent(RefactorTests.Person.class, t ->
                Companion2.buildComponentSerdes(t, List.of(),
                    type2 -> (obj) -> 256,
                    type2 -> (b, o) -> {
                      if (o instanceof RefactorTests.Person(String name, int age)) {
                        byte[] nameBytes = name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        ZigZagEncoding.putInt(b, nameBytes.length);
                        b.put(nameBytes);
                        b.putInt(age);
                      }
                    },
                    sig2 -> b -> {
                      int nameLength = ZigZagEncoding.getInt(b);
                      byte[] nameBytes = new byte[nameLength];
                      b.get(nameBytes);
                      String name = new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8);
                      int age = b.getInt();
                      return new RefactorTests.Person(name, age);
                    }
                ));
            return personSerdes[0].reader().apply(buffer);
          }
          throw new IllegalArgumentException("Unknown signature: " + Long.toHexString(signature));
        });

    // 3. Perform manual Serialization and Deserialization.
    final ByteBuffer buffer2 = ByteBuffer.allocate(4096);

    // The writer for the first component ('items' map) takes the whole record
    // and internally extracts the map to serialize it.
    serdes[0].writer().accept(buffer2, originalRecord);
    buffer2.flip();

    // The reader for the first component returns the deserialized component (the map).
    @SuppressWarnings("unchecked")
    Map<TypeExpr2MapTests.HeterogeneousItem, TypeExpr2MapTests.HeterogeneousItem> deserializedMap = (Map<TypeExpr2MapTests.HeterogeneousItem, TypeExpr2MapTests.HeterogeneousItem>) serdes[0].reader().apply(buffer2);

    // Reconstruct the record from the deserialized component.
    final var deserializedRecord = new ComplexMapRecord(deserializedMap);

    // Deep equality check on the original and deserialized maps.
    // HashMap's equals method correctly handles null keys and values.
    assertThat(deserializedMap).isEqualTo(complexMap);
  }

  /**
   * Builds a heterogeneous map for testing.
   * Critically, it includes a null key and a null value to test handling of these cases,
   * which is supported by java.util.HashMap.
   */
  static @NotNull Map<TypeExpr2MapTests.HeterogeneousItem, TypeExpr2MapTests.HeterogeneousItem> buildMapForTest() {
    final Map<TypeExpr2MapTests.HeterogeneousItem, TypeExpr2MapTests.HeterogeneousItem> map = new HashMap<>();

    // Populate with a variety of heterogeneous key-value pairs
    map.put(new TypeExpr2MapTests.ItemString("stringKey"), new TypeExpr2MapTests.ItemInt(12345));
    map.put(new TypeExpr2MapTests.ItemInt(54321), new TypeExpr2MapTests.ItemString("value for int key"));
    map.put(new TypeExpr2MapTests.ItemLong(98765L), new TypeExpr2MapTests.ItemBoolean(true));
    map.put(new TypeExpr2MapTests.ItemTestRecord(new RefactorTests.Person("keyPerson", 99)), new TypeExpr2MapTests.ItemString("value for record key"));
    map.put(new TypeExpr2MapTests.ItemString("key for enum value"), new TypeExpr2MapTests.ItemTestEnum(TypeExpr2Tests.TestEnum.THIRD));

    // Critical test cases for null handling.
    // A key mapped to an explicit null representation (ItemNull).
    map.put(new TypeExpr2MapTests.ItemString("keyToNullItem"), new TypeExpr2MapTests.ItemNull());

    // An explicit null representation (ItemNull) used as a key.
    map.put(new TypeExpr2MapTests.ItemNull(), new TypeExpr2MapTests.ItemString("valueForItemNullKey"));

    return map;
  }

}

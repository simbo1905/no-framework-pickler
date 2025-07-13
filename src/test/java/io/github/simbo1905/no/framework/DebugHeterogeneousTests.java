// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import io.github.simbo1905.RefactorTests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("auxiliaryclass")
class DebugHeterogeneousTests {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  /// Test data records
  public sealed interface HeterogeneousItem permits
      DebugHeterogeneousTests.ItemString, DebugHeterogeneousTests.ItemInt, DebugHeterogeneousTests.ItemLong,
      DebugHeterogeneousTests.ItemBoolean, DebugHeterogeneousTests.ItemNull, DebugHeterogeneousTests.ItemTestRecord,
      DebugHeterogeneousTests.ItemTestEnum, DebugHeterogeneousTests.ItemOptional, DebugHeterogeneousTests.ItemList {
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

  public record ItemTestRecord(RefactorTests.Person person) implements HeterogeneousItem {
  }

  public record ItemTestEnum(TypeExpr2Tests.TestEnum value) implements HeterogeneousItem {
  }

  public record ItemOptional(Optional<String> value) implements HeterogeneousItem {
  }

  public record ItemList(List<String> value) implements HeterogeneousItem {
  }

  public record ComplexListRecord(List<HeterogeneousItem> items) {
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
    allRecordTypes.add(RefactorTests.Person.class);
    allRecordTypes.add(ComplexListRecord.class);
    final var recordTypeSignatureMap = Companion2.computeRecordTypeSignatures(allRecordTypes);
    final long testEnumSignature = Companion2.hashEnumSignature(TypeExpr2Tests.TestEnum.class);
    final Map<Class<?>, ComponentSerde[]> serdeMap = new HashMap<>();

    ComponentSerde[] serdes = Companion2.buildComponentSerdes(
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
                Companion2.buildComponentSerdes(t, List.of(),
                    type2 -> (o) -> 256,
                    type2 -> (buf, o) -> {
                      if (o instanceof ItemString str) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(String.class));
                        byte[] bytes = str.value().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        ZigZagEncoding.putInt(buf, bytes.length);
                        buf.put(bytes);
                      } else if (o instanceof ItemInt intVal) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(Integer.class));
                        buf.putInt(intVal.value());
                      } else if (o instanceof ItemLong longVal) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(Long.class));
                        buf.putLong(longVal.value());
                      }
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
                Companion2.buildComponentSerdes(t, List.of(),
                    type -> (obj) -> 256,
                    type2 -> (buf, o) -> {
                      if (o instanceof ItemString str) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(String.class));
                        byte[] bytes = str.value().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        ZigZagEncoding.putInt(buf, bytes.length);
                        buf.put(bytes);
                      } else if (o instanceof ItemInt intVal) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(Integer.class));
                        buf.putInt(intVal.value());
                      } else if (o instanceof ItemLong longVal) {
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(Long.class));
                        buf.putLong(longVal.value());
                      }
                    },
                    sig2 -> buf -> {
                      if (targetType == ItemString.class) {
                        int marker = ZigZagEncoding.getInt(buf);
                        if (marker != TypeExpr2.referenceToMarker(String.class)) {
                          throw new IllegalStateException("Expected STRING marker");
                        }
                        int length = ZigZagEncoding.getInt(buf);
                        byte[] bytes = new byte[length];
                        buf.get(bytes);
                        String value = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
                        return value;
                      } else if (targetType == ItemInt.class) {
                        int marker = ZigZagEncoding.getInt(buf);
                        if (marker != TypeExpr2.referenceToMarker(Integer.class)) {
                          throw new IllegalStateException("Expected INTEGER marker");
                        }
                        return buf.getInt();
                      } else if (targetType == ItemLong.class) {
                        int marker = ZigZagEncoding.getInt(buf);
                        if (marker != TypeExpr2.referenceToMarker(Long.class)) {
                          throw new IllegalStateException("Expected LONG marker");
                        }
                        return buf.getLong();
                      }
                      return null;
                    }));
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
    ComponentSerde[] serdes = Companion2.buildComponentSerdes(
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
              ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(String.class));
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
            if (marker != TypeExpr2.referenceToMarker(String.class)) {
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


  @Test
  void testComplexHeterogeneousListWithPickler() {
    // Create a complex list using the sealed interface wrappers
    final List<TypeExpr2Tests.HeterogeneousItem> complexList = new ArrayList<>();
    complexList.add(new TypeExpr2Tests.ItemString("a string"));
    complexList.add(new TypeExpr2Tests.ItemInt(123));
    complexList.add(new TypeExpr2Tests.ItemLong(456L));
    complexList.add(new TypeExpr2Tests.ItemBoolean(true));
    complexList.add(new TypeExpr2Tests.ItemNull()); // Representing null
    complexList.add(new TypeExpr2Tests.ItemTestRecord(new RefactorTests.Person("rec1", 99)));
    complexList.add(new TypeExpr2Tests.ItemTestEnum(TypeExpr2Tests.TestEnum.SECOND));
    complexList.add(new TypeExpr2Tests.ItemOptional(Optional.empty()));
    complexList.add(new TypeExpr2Tests.ItemList(Arrays.asList("nested", "list", null))); // List with nulls included
    final var originalRecord = new TypeExpr2Tests.ComplexListRecord(complexList);

    // Log hashCodes for debugging as requested
    LOGGER.info(() -> "Test: originalRecord.hashCode() = " + originalRecord.hashCode());
    IntStream.range(0, complexList.size()).forEach(i ->
        LOGGER.info(() -> "Test: complexList[" + i + "].hashCode() = " + complexList.get(i).hashCode() + " type=" + complexList.get(i).getClass().getSimpleName())
    );

    // 1. Define all record types in the sealed hierarchy
    final List<Class<?>> sealedTypes = List.of(
        TypeExpr2Tests.ItemString.class, TypeExpr2Tests.ItemInt.class, TypeExpr2Tests.ItemLong.class, TypeExpr2Tests.ItemBoolean.class, TypeExpr2Tests.ItemNull.class,
        TypeExpr2Tests.ItemTestRecord.class, TypeExpr2Tests.ItemTestEnum.class, TypeExpr2Tests.ItemOptional.class, TypeExpr2Tests.ItemList.class
    );

    // Also need Person record type
    final List<Class<?>> allRecordTypes = new ArrayList<>(sealedTypes);
    allRecordTypes.add(RefactorTests.Person.class);
    allRecordTypes.add(TypeExpr2Tests.ComplexListRecord.class);

    final var recordTypeSignatureMap = Companion2.computeRecordTypeSignatures(allRecordTypes);

    // Need enum signature for TestEnum
    final long testEnumSignature = Companion2.hashEnumSignature(TypeExpr2Tests.TestEnum.class);

    // Log signatures for debugging
    recordTypeSignatureMap.forEach((type, sig) ->
        LOGGER.info(() -> "Test: " + type.getSimpleName() + " typeSignature = " + Long.toHexString(sig)));
    LOGGER.info(() -> "Test: TestEnum typeSignature = " + Long.toHexString(testEnumSignature));

    // 2. Create ComponentSerde arrays for all types
    final Map<Class<?>, ComponentSerde[]> serdeMap = new HashMap<>();

    // Build ComponentSerde array with proper resolvers
    ComponentSerde[] serdes = Companion2.buildComponentSerdes(
        TypeExpr2Tests.ComplexListRecord.class,
        List.of(), // No custom handlers
        // Sizer resolver
        type -> (obj) -> {
          if (obj == null) return Long.BYTES; // Null signature

          // For Person record
          if (type == RefactorTests.Person.class) {
            RefactorTests.Person person = (RefactorTests.Person) obj;
            return Long.BYTES + // signature
                Byte.BYTES + ZigZagEncoding.sizeOf(TypeExpr2.referenceToMarker(String.class)) +
                ZigZagEncoding.sizeOf(person.name().length()) + person.name().getBytes(java.nio.charset.StandardCharsets.UTF_8).length + // name
                ZigZagEncoding.sizeOf(TypeExpr2.primitiveToMarker(int.class)) + Integer.BYTES; // age
          }

          // For TestEnum
          if (type == TypeExpr2Tests.TestEnum.class) {
            return Long.BYTES + // signature
                ZigZagEncoding.sizeOf(((Enum<?>) obj).ordinal()); // ordinal
          }

          // For sealed interface implementations
          if (sealedTypes.contains(type)) {
            // Each sealed type is a record with one component
            // Size = signature + component size
            var componentSerdes = serdeMap.computeIfAbsent(type, t ->
                Companion2.buildComponentSerdes(t, List.of(),
                    null, null, null));
            return Long.BYTES + componentSerdes[0].sizer().applyAsInt(obj);
          }

          return 256; // Default for unknown types
        },
        // Writer resolver
        type -> (buffer, obj) -> {
          if (obj == null) {
            LOGGER.fine("WriterResolver: Writing null signature (0L)");
            buffer.putLong(0L);
            return;
          }

          LOGGER.info(() -> "WriterResolver: Writing object with hashCode: " + obj.hashCode() + " type: " + obj.getClass().getSimpleName());

          // For Person record
          if (type == RefactorTests.Person.class) {
            RefactorTests.Person person = (RefactorTests.Person) obj;
            long sig = recordTypeSignatureMap.get(RefactorTests.Person.class);
            LOGGER.fine(() -> "WriterResolver: Writing Person signature " + Long.toHexString(sig));
            buffer.putLong(sig);
            // Write name (nullable String)
            buffer.put(Companion2.NOT_NULL_MARKER);
            ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(String.class));
            byte[] nameBytes = person.name().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ZigZagEncoding.putInt(buffer, nameBytes.length);
            buffer.put(nameBytes);
            // Write age (primitive int)
            ZigZagEncoding.putInt(buffer, TypeExpr2.primitiveToMarker(int.class));
            buffer.putInt(person.age());
            return;
          }

          // For TestEnum
          if (type == TypeExpr2Tests.TestEnum.class) {
            TypeExpr2Tests.TestEnum enumValue = (TypeExpr2Tests.TestEnum) obj;
            LOGGER.fine(() -> "WriterResolver: Writing TestEnum signature " + Long.toHexString(testEnumSignature));
            buffer.putLong(testEnumSignature);
            ZigZagEncoding.putInt(buffer, enumValue.ordinal());
            return;
          }

          // For sealed interface implementations
          if (sealedTypes.contains(type)) {
            long sig = recordTypeSignatureMap.get(type);
            LOGGER.fine(() -> "WriterResolver: Writing " + type.getSimpleName() + " signature " + Long.toHexString(sig));
            buffer.putLong(sig);
            // Write the single component
            var componentSerdes = serdeMap.computeIfAbsent(type, t ->
                Companion2.buildComponentSerdes(t, List.of(),
                    null, null, null));
            componentSerdes[0].writer().accept(buffer, obj);
            return;
          }

          // For HeterogeneousItem interface - delegate to the concrete type
          if (type == TypeExpr2Tests.HeterogeneousItem.class) {
            Class<?> concreteType = obj.getClass();
            LOGGER.fine(() -> "WriterResolver: HeterogeneousItem interface, delegating to concrete type: " + concreteType.getSimpleName());
            long sig = recordTypeSignatureMap.get(concreteType);
            LOGGER.fine(() -> "WriterResolver: Writing " + concreteType.getSimpleName() + " signature " + Long.toHexString(sig));
            buffer.putLong(sig);
            // Write using the concrete type's component serde
            var componentSerdes = serdeMap.computeIfAbsent(concreteType, t ->
                Companion2.buildComponentSerdes(t, List.of(),
                    type2 -> (o) -> 256,
                    type2 -> (buf, o) -> {
                      if (type2 == RefactorTests.Person.class) {
                        RefactorTests.Person person = (RefactorTests.Person) o;
                        // Write name (nullable String)
                        buf.put(Companion2.NOT_NULL_MARKER);
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(String.class));
                        byte[] nameBytes = person.name().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        ZigZagEncoding.putInt(buf, nameBytes.length);
                        buf.put(nameBytes);
                        // Write age (primitive int)
                        ZigZagEncoding.putInt(buf, TypeExpr2.primitiveToMarker(int.class));
                        buf.putInt(person.age());
                      } else if (type2 == TypeExpr2Tests.TestEnum.class) {
                        TypeExpr2Tests.TestEnum enumValue = (TypeExpr2Tests.TestEnum) o;
                        ZigZagEncoding.putInt(buf, enumValue.ordinal());
                      }
                    },
                    sig2 -> buf -> {
                      if (sig2 == testEnumSignature) {
                        int ordinal = ZigZagEncoding.getInt(buf);
                        return TypeExpr2Tests.TestEnum.values()[ordinal];
                      } else if (recordTypeSignatureMap.containsValue(sig2)) {
                        Class<?> targetType = recordTypeSignatureMap.entrySet().stream()
                            .filter(entry -> entry.getValue().equals(sig2))
                            .map(Map.Entry::getKey)
                            .findFirst()
                            .orElse(null);
                        if (targetType == RefactorTests.Person.class) {
                          // Read name
                          byte nullMarker = buf.get();
                          if (nullMarker != Companion2.NOT_NULL_MARKER) {
                            throw new IllegalStateException("Expected NOT_NULL_MARKER for Person.name");
                          }
                          int stringMarker = ZigZagEncoding.getInt(buf);
                          if (stringMarker != TypeExpr2.referenceToMarker(String.class)) {
                            throw new IllegalStateException("Expected STRING marker");
                          }
                          int nameLength = ZigZagEncoding.getInt(buf);
                          byte[] nameBytes = new byte[nameLength];
                          buf.get(nameBytes);
                          String name = new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8);
                          // Read age
                          int intMarker = ZigZagEncoding.getInt(buf);
                          if (intMarker != TypeExpr2.primitiveToMarker(int.class)) {
                            throw new IllegalStateException("Expected INT marker");
                          }
                          int age = buf.getInt();
                          return new RefactorTests.Person(name, age);
                        }
                      }
                      return null;
                    }));
            if (componentSerdes.length > 0) {
              componentSerdes[0].writer().accept(buffer, obj);
            }
            return;
          }

          throw new IllegalArgumentException("Unknown type for writer: " + type);
        },
        // Reader resolver by signature
        signature -> buffer -> {
          if (signature == 0L) {
            return null; // Null object
          }

          // Check if it's TestEnum
          if (signature == testEnumSignature) {
            LOGGER.fine(() -> "ReaderResolver: Reading TestEnum from signature " + Long.toHexString(signature));
            int ordinal = ZigZagEncoding.getInt(buffer);
            return TypeExpr2Tests.TestEnum.values()[ordinal];
          }

          // Find the record type by signature
          Class<?> targetType = recordTypeSignatureMap.entrySet().stream()
              .filter(entry -> entry.getValue().equals(signature))
              .map(Map.Entry::getKey)
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("Unknown signature: " + Long.toHexString(signature)));

          LOGGER.fine(() -> "ReaderResolver: Reading type " + targetType.getSimpleName() + " from signature " + Long.toHexString(signature));

          // For Person record
          if (targetType == RefactorTests.Person.class) {
            // Read name
            byte nullMarker = buffer.get();
            if (nullMarker != Companion2.NOT_NULL_MARKER) {
              throw new IllegalStateException("Expected NOT_NULL_MARKER for Person.name");
            }
            int stringMarker = ZigZagEncoding.getInt(buffer);
            if (stringMarker != TypeExpr2.referenceToMarker(String.class)) {
              throw new IllegalStateException("Expected STRING marker");
            }
            int nameLength = ZigZagEncoding.getInt(buffer);
            byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            String name = new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8);

            // Read age
            int intMarker = ZigZagEncoding.getInt(buffer);
            if (intMarker != TypeExpr2.primitiveToMarker(int.class)) {
              throw new IllegalStateException("Expected INT marker");
            }
            int age = buffer.getInt();

            return new RefactorTests.Person(name, age);
          }

          // For sealed interface implementations
          if (sealedTypes.contains(targetType)) {
            // Read the single component
            var componentSerdes = serdeMap.computeIfAbsent(targetType, t ->
                Companion2.buildComponentSerdes(t, List.of(),
                    type -> (obj) -> 256,
                    type2 -> (buf, o) -> {
                      if (type2 == RefactorTests.Person.class) {
                        RefactorTests.Person person = (RefactorTests.Person) o;
                        // Write name (nullable String)
                        buf.put(Companion2.NOT_NULL_MARKER);
                        ZigZagEncoding.putInt(buf, TypeExpr2.referenceToMarker(String.class));
                        byte[] nameBytes = person.name().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        ZigZagEncoding.putInt(buf, nameBytes.length);
                        buf.put(nameBytes);
                        // Write age (primitive int)
                        ZigZagEncoding.putInt(buf, TypeExpr2.primitiveToMarker(int.class));
                        buf.putInt(person.age());
                      } else if (type2 == TypeExpr2Tests.TestEnum.class) {
                        TypeExpr2Tests.TestEnum enumValue = (TypeExpr2Tests.TestEnum) o;
                        ZigZagEncoding.putInt(buf, enumValue.ordinal());
                      }
                    },
                    sig2 -> buf -> {
                      if (sig2 == testEnumSignature) {
                        int ordinal = ZigZagEncoding.getInt(buf);
                        return TypeExpr2Tests.TestEnum.values()[ordinal];
                      } else if (recordTypeSignatureMap.containsValue(sig2)) {
                        Class<?> targetType2 = recordTypeSignatureMap.entrySet().stream()
                            .filter(entry -> entry.getValue().equals(sig2))
                            .map(Map.Entry::getKey)
                            .findFirst()
                            .orElse(null);
                        if (targetType2 == RefactorTests.Person.class) {
                          // Read name
                          byte nullMarker = buf.get();
                          if (nullMarker != Companion2.NOT_NULL_MARKER) {
                            throw new IllegalStateException("Expected NOT_NULL_MARKER for Person.name");
                          }
                          int stringMarker = ZigZagEncoding.getInt(buf);
                          if (stringMarker != TypeExpr2.referenceToMarker(String.class)) {
                            throw new IllegalStateException("Expected STRING marker");
                          }
                          int nameLength = ZigZagEncoding.getInt(buf);
                          byte[] nameBytes = new byte[nameLength];
                          buf.get(nameBytes);
                          String name = new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8);
                          // Read age
                          int intMarker = ZigZagEncoding.getInt(buf);
                          if (intMarker != TypeExpr2.primitiveToMarker(int.class)) {
                            throw new IllegalStateException("Expected INT marker");
                          }
                          int age = buf.getInt();
                          return new RefactorTests.Person(name, age);
                        }
                      }
                      return null;
                    }));
            Object componentValue = componentSerdes[0].reader().apply(buffer);

            // Construct the appropriate sealed type
            if (targetType == TypeExpr2Tests.ItemString.class) {
              return new TypeExpr2Tests.ItemString((String) componentValue);
            } else if (targetType == TypeExpr2Tests.ItemInt.class) {
              return new TypeExpr2Tests.ItemInt((Integer) componentValue);
            } else if (targetType == TypeExpr2Tests.ItemLong.class) {
              return new TypeExpr2Tests.ItemLong((Long) componentValue);
            } else if (targetType == TypeExpr2Tests.ItemBoolean.class) {
              return new TypeExpr2Tests.ItemBoolean((Boolean) componentValue);
            } else if (targetType == TypeExpr2Tests.ItemNull.class) {
              return new TypeExpr2Tests.ItemNull();
            } else if (targetType == TypeExpr2Tests.ItemTestRecord.class) {
              return new TypeExpr2Tests.ItemTestRecord((RefactorTests.Person) componentValue);
            } else if (targetType == TypeExpr2Tests.ItemTestEnum.class) {
              return new TypeExpr2Tests.ItemTestEnum((TypeExpr2Tests.TestEnum) componentValue);
            } else if (targetType == TypeExpr2Tests.ItemOptional.class) {
              @SuppressWarnings("unchecked")
              Optional<String> opt = (Optional<String>) componentValue;
              return new TypeExpr2Tests.ItemOptional(opt);
            } else if (targetType == TypeExpr2Tests.ItemList.class) {
              @SuppressWarnings("unchecked")
              List<String> list = (List<String>) componentValue;
              return new TypeExpr2Tests.ItemList(list);
            }
          }

          throw new IllegalArgumentException("Unknown target type: " + targetType);
        }
    );

    // Store the ComplexListRecord serdes in the map
    serdeMap.put(TypeExpr2Tests.ComplexListRecord.class, serdes);

    ByteBuffer buffer = ByteBuffer.allocate(4096);

    // Serialize
    serdes[0].writer().accept(buffer, originalRecord);
    buffer.flip();

    // Deserialize
    @SuppressWarnings("unchecked")
    List<TypeExpr2Tests.HeterogeneousItem> deserializedList = (List<TypeExpr2Tests.HeterogeneousItem>) serdes[0].reader().apply(buffer);

    // Assert
    assertThat(deserializedList).hasSize(complexList.size());
    IntStream.range(0, complexList.size()).forEach(i ->
        assertThat(deserializedList.get(i)).isEqualTo(complexList.get(i))
    );
  }

}

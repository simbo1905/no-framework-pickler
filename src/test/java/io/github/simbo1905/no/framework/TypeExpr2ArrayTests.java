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
import java.util.*;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("auxiliaryclass")
class TypeExpr2ArrayTests {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  public sealed interface HeterogeneousItem permits
      ItemString, ItemInt, ItemLong,
      ItemBoolean, ItemNull, ItemTestRecord,
      ItemTestEnum, ItemOptional, ItemList, ItemArray {
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

  public record ItemArray(String[] value) implements HeterogeneousItem {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ItemArray itemArray = (ItemArray) o;
      return Arrays.equals(value, itemArray.value);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(value);
    }

    @Override
    public String toString() {
      return "ItemArray[value=" + Arrays.toString(value) + "]";
    }
  }

  // Test data record holding a map.
  public record ComplexArrayRecord(HeterogeneousItem[] items) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ComplexArrayRecord that = (ComplexArrayRecord) o;
      return Arrays.equals(items, that.items);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(items);
    }

    @Override
    public String toString() {
      return "ComplexArrayRecord[items=" + Arrays.toString(items) + "]";
    }
  }

  // Same resolver setup as main test
  final List<Class<?>> sealedTypes = List.of(
      ItemString.class, ItemInt.class, ItemLong.class, ItemBoolean.class, ItemNull.class,
      ItemTestRecord.class, ItemTestEnum.class, ItemOptional.class, ItemList.class, ItemArray.class
  );

  @SuppressWarnings("unchecked")
  @Test
  void testComplexHeterogeneousArrayManually() {
    final HeterogeneousItem[] complexArray = buildArrayForTest();
    final var originalRecord = new ComplexArrayRecord(complexArray);

    LOGGER.info(() -> "Test: originalRecord.hashCode() = " + originalRecord.hashCode());
    for (int i = 0; i < complexArray.length; i++) {
      final int index = i;
      LOGGER.info(() -> "Test: complexArray[" + index + "].hashCode() = " + complexArray[index].hashCode() + " type=" + complexArray[index].getClass().getSimpleName());
    }

    // 1. Define all record and enum types that need explicit signature handling.
    final List<Class<?>> allRecordTypes = new ArrayList<>(sealedTypes);
    allRecordTypes.add(ComplexArrayRecord.class);
    allRecordTypes.add(RefactorTests.Person.class);

    final var recordTypeSignatureMap = Companion2.computeRecordTypeSignatures(allRecordTypes);
    final long testEnumSignature = Companion2.hashEnumSignature(TypeExpr2Tests.TestEnum.class);

    // Log signatures for debugging.
    recordTypeSignatureMap.forEach((type, sig) ->
        LOGGER.info(() -> "Test: " + type.getSimpleName() + " typeSignature = " + Long.toHexString(sig)));
    LOGGER.info(() -> "Test: TestEnum typeSignature = " + Long.toHexString(testEnumSignature));

    final Map<Class<?>, ComponentSerde[]> serdeMap = new HashMap<>();

    // Create recursive resolvers using holders to break circular dependency
    final SizerResolver[] sizerResolverHolder = new SizerResolver[1];
    final WriterResolver[] writerResolverHolder = new WriterResolver[1];
    final ReaderResolver[] readerResolverHolder = new ReaderResolver[1];

    SizerResolver sizerResolver = type -> (obj) -> {
      if (obj == null) return Long.BYTES; // Null signature

      if (type == RefactorTests.Person.class) {
        RefactorTests.Person person = (RefactorTests.Person) obj;
        return Long.BYTES + // signature
            Byte.BYTES + ZigZagEncoding.sizeOf(TypeExpr2.referenceToMarker(String.class)) +
            ZigZagEncoding.sizeOf(person.name().length()) + person.name().getBytes(java.nio.charset.StandardCharsets.UTF_8).length + // name
            ZigZagEncoding.sizeOf(TypeExpr2.primitiveToMarker(int.class)) + Integer.BYTES; // age
      }

      if (type == TypeExpr2Tests.TestEnum.class) {
        return Long.BYTES + ZigZagEncoding.sizeOf(((Enum<?>) obj).ordinal());
      }

      if (sealedTypes.contains(type)) {
        var componentSerdes = serdeMap.computeIfAbsent(type, t ->
            Companion2.buildComponentSerdes(t, List.of(), sizerResolverHolder[0], writerResolverHolder[0], readerResolverHolder[0]));
        if (componentSerdes.length == 0) return Long.BYTES;
        return Long.BYTES + componentSerdes[0].sizer().applyAsInt(obj);
      }
      return 256;
    };
    sizerResolverHolder[0] = sizerResolver;

    WriterResolver writerResolver = type -> (buffer, obj) -> {
      if (obj == null) {
        buffer.putLong(0L);
        return;
      }

      LOGGER.fine(() -> "WriterResolver: Writing object with hashCode: " + obj.hashCode() + " type: " + obj.getClass().getSimpleName());

      Class<?> concreteType = obj.getClass();

      if (concreteType == RefactorTests.Person.class) {
        RefactorTests.Person person = (RefactorTests.Person) obj;
        buffer.putLong(recordTypeSignatureMap.get(RefactorTests.Person.class));
        // Write components of Person
        var personSerdes = serdeMap.computeIfAbsent(RefactorTests.Person.class, t ->
            Companion2.buildComponentSerdes(t, List.of(), sizerResolverHolder[0], writerResolverHolder[0], readerResolverHolder[0]));
        personSerdes[0].writer().accept(buffer, person); // name
        personSerdes[1].writer().accept(buffer, person); // age
        return;
      }

      if (concreteType == TypeExpr2Tests.TestEnum.class) {
        buffer.putLong(testEnumSignature);
        ZigZagEncoding.putInt(buffer, ((TypeExpr2Tests.TestEnum) obj).ordinal());
        return;
      }

      if (sealedTypes.contains(concreteType)) {
        buffer.putLong(recordTypeSignatureMap.get(concreteType));
        var componentSerdes = serdeMap.computeIfAbsent(concreteType, t ->
            Companion2.buildComponentSerdes(t, List.of(), sizerResolverHolder[0], writerResolverHolder[0], readerResolverHolder[0]));
        if (componentSerdes.length > 0) {
          componentSerdes[0].writer().accept(buffer, obj);
        }
        return;
      }

      if (type == HeterogeneousItem.class) {
        writerResolverHolder[0].resolveWriter(concreteType).accept(buffer, obj);
        return;
      }

      throw new IllegalArgumentException("Unknown type for writer: " + type);
    };
    writerResolverHolder[0] = writerResolver;

    ReaderResolver readerResolver = signature -> buffer -> {
      if (signature == 0L) return null;

      if (signature == testEnumSignature) {
        int ordinal = ZigZagEncoding.getInt(buffer);
        return TypeExpr2Tests.TestEnum.values()[ordinal];
      }

      Class<?> targetType = recordTypeSignatureMap.entrySet().stream()
          .filter(entry -> entry.getValue().equals(signature))
          .map(Map.Entry::getKey)
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Unknown signature: " + Long.toHexString(signature)));

      if (targetType == RefactorTests.Person.class) {
        var personSerdes = serdeMap.computeIfAbsent(RefactorTests.Person.class, t ->
            Companion2.buildComponentSerdes(t, List.of(), sizerResolverHolder[0], writerResolverHolder[0], readerResolverHolder[0]));
        String name = (String) personSerdes[0].reader().apply(buffer);
        int age = (int) personSerdes[1].reader().apply(buffer);
        return new RefactorTests.Person(name, age);
      }

      if (sealedTypes.contains(targetType)) {
        var componentSerdes = serdeMap.computeIfAbsent(targetType, t ->
            Companion2.buildComponentSerdes(t, List.of(), sizerResolverHolder[0], writerResolverHolder[0], readerResolverHolder[0]));

        if (componentSerdes.length == 0) {
          if (targetType == ItemNull.class) return new ItemNull();
          throw new IllegalStateException("Unhandled zero-component record: " + targetType);
        }

        Object componentValue = componentSerdes[0].reader().apply(buffer);

        // Construct the appropriate sealed type
        if (targetType == ItemString.class) return new ItemString((String) componentValue);
        if (targetType == ItemInt.class) return new ItemInt((Integer) componentValue);
        if (targetType == ItemLong.class) return new ItemLong((Long) componentValue);
        if (targetType == ItemBoolean.class) return new ItemBoolean((Boolean) componentValue);
        if (targetType == ItemTestRecord.class) return new ItemTestRecord((RefactorTests.Person) componentValue);
        if (targetType == ItemTestEnum.class) return new ItemTestEnum((TypeExpr2Tests.TestEnum) componentValue);
        if (targetType == ItemOptional.class) return new ItemOptional((Optional<String>) componentValue);
        if (targetType == ItemList.class) return new ItemList((List<String>) componentValue);
        if (targetType == ItemArray.class) return new ItemArray((String[]) componentValue);
      }

      throw new IllegalArgumentException("Unknown target type for construction: " + targetType);
    };
    readerResolverHolder[0] = readerResolver;

    // 2. Build component serdes for the ComplexMapRecord
    ComponentSerde[] serdes = Companion2.buildComponentSerdes(
        ComplexArrayRecord.class,
        List.of(),
        sizerResolver,
        writerResolver,
        readerResolver
    );

    // 3. Perform manual Serialization and Deserialization.
    final ByteBuffer buffer = ByteBuffer.allocate(4096);
    serdes[0].writer().accept(buffer, originalRecord);
    buffer.flip();

    HeterogeneousItem[] deserializedArray = (HeterogeneousItem[]) serdes[0].reader().apply(buffer);

    final var deserializedRecord = new ComplexArrayRecord(deserializedArray);
    // assertThat(deserializedRecord).isEqualTo(originalRecord);
    assertThat(deserializedRecord.items).isEqualTo(originalRecord.items);
  }

  /**
   * Builds a heterogeneous map for testing.
   */
  static @NotNull HeterogeneousItem[] buildArrayForTest() {
    final List<HeterogeneousItem> list = new ArrayList<>();
    list.add(new ItemString("a string"));
    list.add(new ItemInt(123));
    list.add(new ItemLong(456L));
    list.add(new ItemBoolean(true));
    list.add(new ItemNull()); // Representing null
    list.add(new ItemTestRecord(new RefactorTests.Person("rec1", 99)));
    list.add(new ItemTestEnum(TypeExpr2Tests.TestEnum.SECOND));
    list.add(new ItemOptional(Optional.empty()));
    list.add(new ItemList(Arrays.asList("nested", "list", null))); // List with nulls included
    list.add(new ItemArray(new String[]{"nested", "array", null})); // Array with nulls included
    return list.toArray(new HeterogeneousItem[0]);
  }
}

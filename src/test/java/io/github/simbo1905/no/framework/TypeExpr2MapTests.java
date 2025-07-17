// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.BaselineTests;
import io.github.simbo1905.LoggingControl;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

class TypeExpr2MapTests {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  public sealed interface HeterogeneousItem permits
      ItemString, ItemInt, ItemLong,
      ItemBoolean, ItemNull, ItemTestRecord,
      ItemTestEnum, ItemOptional, ItemList {
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

  public record ItemTestRecord(BaselineTests.Person person) implements HeterogeneousItem {
  }

  public record ItemTestEnum(TypeExpr2Tests.TestEnum value) implements HeterogeneousItem {
  }

  public record ItemOptional(Optional<String> value) implements HeterogeneousItem {
  }

  public record ItemList(List<String> value) implements HeterogeneousItem {
  }

  // Test data record holding a map.
  public record ComplexMapRecord(Map<HeterogeneousItem, HeterogeneousItem> items) {
  }

  // Same resolver setup as main test
  final List<Class<?>> sealedTypes = List.of(
      ItemString.class, ItemInt.class, ItemLong.class, ItemBoolean.class, ItemNull.class,
      ItemTestRecord.class, ItemTestEnum.class, ItemOptional.class, ItemList.class
  );

  @SuppressWarnings("unchecked")
  @Test
  void testComplexHeterogeneousMapManually() {
    final Map<HeterogeneousItem, HeterogeneousItem> complexMap = buildMapForTest();
    final var originalRecord = new ComplexMapRecord(complexMap);

    LOGGER.info(() -> "Test: originalRecord.hashCode() = " + originalRecord.hashCode());
    originalRecord.items.forEach((k, v) ->
        LOGGER.info(() -> String.format("Test: Map item K=[%s], V=[%s], V-Type=[%s]",
            k, v, (v == null ? "null" : v.getClass().getSimpleName())))
    );

    // 1. Define all record and enum types that need explicit signature handling.
    final List<Class<?>> allRecordTypes = new ArrayList<>(sealedTypes);
    allRecordTypes.add(ComplexMapRecord.class);
    allRecordTypes.add(BaselineTests.Person.class);

    final var recordTypeSignatureMap = Companion.computeRecordTypeSignatures(allRecordTypes);
    final long testEnumSignature = Companion.hashEnumSignature(TypeExpr2Tests.TestEnum.class);

    // Log signatures for debugging.
    recordTypeSignatureMap.forEach((type, sig) ->
        LOGGER.info(() -> "Test: " + type.getSimpleName() + " typeSignature = " + Long.toHexString(sig)));
    LOGGER.info(() -> "Test: TestEnum typeSignature = " + Long.toHexString(testEnumSignature));

    final Map<Class<?>, ComponentSerde[]> serdeMap = new HashMap<>();

    // Create placeholders for the resolvers to avoid circular dependencies.
    final SizerResolver[] sizerResolverHolder = new SizerResolver[1];
    final WriterResolver[] writerResolverHolder = new WriterResolver[1];
    final ReaderResolver[] readerResolverHolder = new ReaderResolver[1];

    SizerResolver sizerResolver = type -> (obj) -> {
      if (obj == null) return Long.BYTES; // Null signature

      if (type == BaselineTests.Person.class) {
        BaselineTests.Person person = (BaselineTests.Person) obj;
        return Long.BYTES + // signature
            Byte.BYTES + ZigZagEncoding.sizeOf(Companion.referenceToMarker(String.class)) +
            ZigZagEncoding.sizeOf(person.name().length()) + person.name().getBytes(java.nio.charset.StandardCharsets.UTF_8).length + // name
            ZigZagEncoding.sizeOf(Companion.primitiveToMarker(int.class)) + Integer.BYTES; // age
      }

      if (type == TypeExpr2Tests.TestEnum.class) {
        return Long.BYTES + ZigZagEncoding.sizeOf(((Enum<?>) obj).ordinal());
      }

      if (sealedTypes.contains(type)) {
        var componentSerdes = serdeMap.computeIfAbsent(type, t ->
            Companion.buildComponentSerdes(t, List.of(), sizerResolverHolder[0], writerResolverHolder[0], readerResolverHolder[0]));
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

      Class<?> concreteType = obj.getClass();

      if (concreteType == BaselineTests.Person.class) {
        BaselineTests.Person person = (BaselineTests.Person) obj;
        buffer.putLong(recordTypeSignatureMap.get(BaselineTests.Person.class));
        // Write components of Person
        var personSerdes = serdeMap.computeIfAbsent(BaselineTests.Person.class, t ->
            Companion.buildComponentSerdes(t, List.of(), sizerResolverHolder[0], writerResolverHolder[0], readerResolverHolder[0]));
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
            Companion.buildComponentSerdes(t, List.of(), sizerResolverHolder[0], writerResolverHolder[0], readerResolverHolder[0]));
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

      if (targetType == BaselineTests.Person.class) {
        var personSerdes = serdeMap.computeIfAbsent(BaselineTests.Person.class, t ->
            Companion.buildComponentSerdes(t, List.of(), sizerResolverHolder[0], writerResolverHolder[0], readerResolverHolder[0]));
        String name = (String) personSerdes[0].reader().apply(buffer);
        int age = (int) personSerdes[1].reader().apply(buffer);
        return new BaselineTests.Person(name, age);
      }

      if (sealedTypes.contains(targetType)) {
        var componentSerdes = serdeMap.computeIfAbsent(targetType, t ->
            Companion.buildComponentSerdes(t, List.of(), sizerResolverHolder[0], writerResolverHolder[0], readerResolverHolder[0]));

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
        if (targetType == ItemTestRecord.class) return new ItemTestRecord((BaselineTests.Person) componentValue);
        if (targetType == ItemTestEnum.class) return new ItemTestEnum((TypeExpr2Tests.TestEnum) componentValue);
        if (targetType == ItemOptional.class) return new ItemOptional((Optional<String>) componentValue);
        if (targetType == ItemList.class) return new ItemList((List<String>) componentValue);
      }

      throw new IllegalArgumentException("Unknown target type for construction: " + targetType);
    };
    readerResolverHolder[0] = readerResolver;

    // 2. Build component serdes for the ComplexMapRecord
    ComponentSerde[] serdes = Companion.buildComponentSerdes(
        ComplexMapRecord.class,
        List.of(),
        sizerResolver,
        writerResolver,
        readerResolver
    );

    // 3. Perform manual Serialization and Deserialization.
    final ByteBuffer buffer = ByteBuffer.allocate(4096);
    serdes[0].writer().accept(buffer, originalRecord);
    buffer.flip();

    @SuppressWarnings("unchecked")
    Map<HeterogeneousItem, HeterogeneousItem> deserializedMap = (Map<HeterogeneousItem, HeterogeneousItem>) serdes[0].reader().apply(buffer);

    final var deserializedRecord = new ComplexMapRecord(deserializedMap);
    assertThat(deserializedRecord).isEqualTo(originalRecord);
  }

  /**
   * Builds a heterogeneous map for testing.
   */
  static @NotNull Map<HeterogeneousItem, HeterogeneousItem> buildMapForTest() {
    final Map<HeterogeneousItem, HeterogeneousItem> map = new HashMap<>();
    map.put(new ItemString("stringKey"), new ItemInt(12345));
    map.put(new ItemInt(54321), new ItemString("value for int key"));
    map.put(new ItemLong(98765L), new ItemBoolean(true));
    map.put(new ItemTestRecord(new BaselineTests.Person("keyPerson", 99)), new ItemString("value for record key"));
    map.put(new ItemString("key for enum value"), new ItemTestEnum(TypeExpr2Tests.TestEnum.THIRD));
    map.put(new ItemString("keyToNullItem"), new ItemNull());
    map.put(new ItemNull(), new ItemString("valueForItemNullKey"));
    return map;
  }
}

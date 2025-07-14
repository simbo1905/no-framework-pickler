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

    SizerResolver typeSizerResolver = null;
    WriterResolver typeWriterResolver = null;
    ReaderResolver typeReaderResolver = null;

    // 2. Build component serdes for the ComplexMapRecord
    ComponentSerde[] serdes = Companion2.buildComponentSerdes(
        ComplexMapRecord.class,
        List.of(),
        typeSizerResolver,
        typeWriterResolver,
        typeReaderResolver
    );

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

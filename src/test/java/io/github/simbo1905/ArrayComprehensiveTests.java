// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.*;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive array serialization tests covering various complex scenarios
 * including boxed types, records, enums, varint encoding, nested containers,
 * and deeply nested structures.
 */
public class ArrayComprehensiveTests {

  @BeforeAll
  static void setupLogging() {
    // Copy LoggingControl code inline
    String logLevel = System.getProperty("java.util.logging.ConsoleHandler.level");
    Level level = (logLevel != null) ? Level.parse(logLevel) : Level.WARNING;

    Logger rootLogger = Logger.getLogger("");

    for (Handler handler : rootLogger.getHandlers()) {
      rootLogger.removeHandler(handler);
    }

    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(level);
    consoleHandler.setFormatter(new java.util.logging.Formatter() {
      @Override
      public String format(LogRecord record) {
        return String.format("%-7s %s - %s%n", record.getLevel(), record.getLoggerName(), record.getMessage());
      }
    });

    rootLogger.addHandler(consoleHandler);
    rootLogger.setLevel(level);
  }

  // Test data types
  public enum Color implements Serializable {RED, GREEN, BLUE, YELLOW, PURPLE}

  public record SimpleRecord(String name, int value) implements Serializable {
  }

  public record ComplexRecord(
      String id,
      List<Integer> numbers,
      Map<String, Double> scores
  ) implements Serializable {
  }

  // Container records for various array scenarios
  public record BoxedArraysRecord(
      Integer[] integers,
      Boolean[] booleans,
      Double[] doubles,
      Long[] longs,
      Character[] characters
  ) implements Serializable {
  }

  public record RecordArrayRecord(
      SimpleRecord[] simpleRecords,
      ComplexRecord[] complexRecords
  ) implements Serializable {
  }

  public record EnumArrayRecord(
      Color[] colors,
      Color[][] colorMatrix
  ) implements Serializable {
  }

  public record VarintArrayRecord(
      int[] smallInts,  // Should use varint encoding
      int[] largeInts,  // Should not use varint encoding
      long[] mixedLongs // Mix of small and large values
  ) implements Serializable {
  }

  public record OptionalArrayRecord(
      Optional<Integer>[] optionalInts,
      Optional<String>[] optionalStrings,
      Optional<SimpleRecord>[] optionalRecords
  ) implements Serializable {
  }

  public record CollectionArrayRecord(
      List<String>[] stringLists,
      List<Optional<Integer>>[] optionalIntLists,
      Set<Double>[] doubleSets
  ) implements Serializable {
  }

  public record MapArrayRecord(
      Map<Integer, String>[] intStringMaps,
      Map<String, int[]>[] stringIntArrayMaps,
      Map<Long, List<String>>[] complexMaps
  ) implements Serializable {
  }

  public record DeepNestedArrayRecord(
      int[][][] threeDimInts,
      List<Map<String, int[]>>[] crazyNested,
      Map<String, List<Optional<Integer>[]>>[] ultraNested
  ) implements Serializable {
  }

  public record MixedContainerRecord(
      List<int[]> intArrayList,
      Map<String, Color[]> enumArrayMap,
      Set<Optional<Double>[]> optionalArraySet,
      int[][] beforeArray,
      Map<Integer, List<String[]>> afterArray
  ) implements Serializable {
  }

  public record ExtremeSizeRecord(
      int[] empty,
      String[] single,
      Double[] large,
      List<Integer>[] largeListArray
  ) implements Serializable {
  }

  public record NullPatternRecord(
      String[] allNulls,
      Integer[] startNull,
      Double[] endNull,
      Boolean[] alternatingNull,
      SimpleRecord[] sparseRecords
  ) implements Serializable {
  }

  // Additional test records for missing coverage
  public record StringArrayRecord(
      String[] basicStrings,
      String[][] nestedStrings,
      String[] emptyStrings,
      String[] unicodeStrings
  ) implements Serializable {
  }

  public record UUIDArrayRecord(
      UUID[] uuids,
      UUID[][] nestedUuids
  ) implements Serializable {
  }

  public record PrimitiveEdgeCaseRecord(
      byte[] boundaryBytes,
      char[] unicodeChars,
      float[] specialFloats,
      double[] specialDoubles
  ) implements Serializable {
  }

  public record NestedPrimitiveArrayRecord(
      int[][] matrix2d,
      double[][] doubles2d,
      byte[][][] bytes3d
  ) implements Serializable {
  }

  public record MixedPrimitiveObjectArrayRecord(
      int[] primitiveInts,
      Integer[] boxedInts,
      String[] strings,
      SimpleRecord[] records,
      double[][] nestedDoubles
  ) implements Serializable {
  }

  public record EmptyArraysRecord(
      Color[] emptyColors,
      SimpleRecord[] emptyRecords,
      int[] emptyInts,
      String[] emptyStrings,
      List<String>[] emptyLists
  ) implements Serializable {
  }

  public record SingleElementArraysRecord(
      Color[] singleColor,
      SimpleRecord[] singleRecord,
      int[] singleInt,
      String[] singleString
  ) implements Serializable {
  }

  @Test
  void testBoxedArrays() {
    LOGGER.fine("Testing boxed type arrays");

    BoxedArraysRecord original = new BoxedArraysRecord(
        new Integer[]{1, 2, 3, null, 5, -100, Integer.MAX_VALUE},
        new Boolean[]{true, false, null, true, false},
        new Double[]{1.1, 2.2, null, Double.MAX_VALUE, Double.MIN_VALUE},
        new Long[]{1L, null, Long.MAX_VALUE, Long.MIN_VALUE, 0L},
        new Character[]{'a', 'Z', null, '中'}
    );

    testRoundTrip(original, BoxedArraysRecord.class);
  }

  @Test
  void testRecordArrays() {
    LOGGER.fine("Testing arrays of records");

    SimpleRecord[] simpleRecords = {
        new SimpleRecord("first", 1),
        new SimpleRecord("second", 2),
        null,
        new SimpleRecord("", Integer.MIN_VALUE)
    };

    ComplexRecord[] complexRecords = {
        new ComplexRecord("c1",
            List.of(1, 2, 3),
            Map.of("score", 95.5, "rating", 4.8)),
        null,
        new ComplexRecord("c2",
            Collections.emptyList(),
            Collections.emptyMap()),
        new ComplexRecord("c3",
            Arrays.asList(null, 42, null),
            new HashMap<>() {{
              put("key", null);
            }})
    };

    RecordArrayRecord original = new RecordArrayRecord(simpleRecords, complexRecords);
    testRoundTrip(original, RecordArrayRecord.class);
  }

  @Test
  void testEnumArrays() {
    LOGGER.fine("Testing enum arrays");

    Color[] colors = {Color.RED, null, Color.BLUE, Color.GREEN, Color.YELLOW};
    Color[][] colorMatrix = {
        {Color.RED, Color.GREEN},
        null,
        {null, Color.BLUE, null},
        {}
    };

    EnumArrayRecord original = new EnumArrayRecord(colors, colorMatrix);
    testRoundTrip(original, EnumArrayRecord.class);
  }

  @Test
  void testVarintEncoding() {
    LOGGER.fine("Testing varint encoding scenarios");

    // Small ints that should use varint encoding (0-127)
    int[] smallInts = IntStream.range(0, 100).toArray();

    // Large ints that should not benefit from varint encoding
    int[] largeInts = IntStream.range(0, 50)
        .map(i -> Integer.MAX_VALUE - i)
        .toArray();

    // Mixed longs
    long[] mixedLongs = {
        0L, 1L, 127L, 128L, 255L, 256L,
        -1L, -128L, -129L,
        Long.MAX_VALUE, Long.MIN_VALUE,
        1000000L, -1000000L
    };

    VarintArrayRecord original = new VarintArrayRecord(smallInts, largeInts, mixedLongs);
    testRoundTrip(original, VarintArrayRecord.class);
  }

  @Test
  void testOptionalArrays() {
    LOGGER.fine("Testing arrays of Optional");

    Optional<Integer>[] optionalInts = new Optional[]{
        Optional.of(1),
        Optional.empty(),
        Optional.of(-42),
        null,
        Optional.of(Integer.MAX_VALUE)
    };

    Optional<String>[] optionalStrings = new Optional[]{
        Optional.of("hello"),
        Optional.empty(),
        null,
        Optional.of(""),
        Optional.of("unicode: 你好 🌍")
    };

    Optional<SimpleRecord>[] optionalRecords = new Optional[]{
        Optional.of(new SimpleRecord("test", 1)),
        Optional.empty(),
        null,
        Optional.of(new SimpleRecord(null, 0))
    };

    OptionalArrayRecord original = new OptionalArrayRecord(
        optionalInts, optionalStrings, optionalRecords
    );
    testRoundTrip(original, OptionalArrayRecord.class);
  }

  @Test
  void testCollectionArrays() {
    LOGGER.fine("Testing arrays of collections");

    List<String>[] stringLists = new List[]{
        List.of("a", "b", "c"),
        Collections.emptyList(),
        null,
        Arrays.asList("bool", null, "z")
    };

    List<Optional<Integer>>[] optionalIntLists = new List[]{
        List.of(Optional.of(1), Optional.empty(), Optional.of(3)),
        null,
        Collections.emptyList(),
        Arrays.asList(null, Optional.of(42), Optional.empty())
    };

    Set<Double>[] doubleSets = new Set[]{
        Set.of(1.1, 2.2, 3.3),
        Collections.emptySet(),
        null,
        new HashSet<>(Arrays.asList(4.4, null, 5.5))
    };

    CollectionArrayRecord original = new CollectionArrayRecord(
        stringLists, optionalIntLists, doubleSets
    );
    testRoundTrip(original, CollectionArrayRecord.class);
  }

  @Test
  void testMapArrays() {
    LOGGER.fine("Testing arrays of maps");

    Map<Integer, String>[] intStringMaps = new Map[]{
        Map.of(1, "one", 2, "two"),
        Collections.emptyMap(),
        null,
        new HashMap<>() {{
          put(3, null);
          put(null, "null-key");
        }}
    };

    Map<String, int[]>[] stringIntArrayMaps = new Map[]{
        Map.of("array1", new int[]{1, 2, 3}, "array2", new int[]{}),
        null,
        Collections.emptyMap(),
        Map.of("single", new int[]{42})
    };

    Map<Long, List<String>>[] complexMaps = new Map[]{
        Map.of(1L, List.of("a", "b"), 2L, Collections.emptyList()),
        null,
        new HashMap<>() {{
          put(3L, Arrays.asList("bool", null, "z"));
          put(null, null);
        }}
    };

    MapArrayRecord original = new MapArrayRecord(
        intStringMaps, stringIntArrayMaps, complexMaps
    );
    testRoundTrip(original, MapArrayRecord.class);
  }

  @Test
  void testDeepNestedArrays() {
    LOGGER.fine("Testing deeply nested array structures");

    int[][][] threeDimInts = {
        {{1, 2}, {3, 4}},
        null,
        {{5}, null, {6, 7, 8}},
        {}
    };

    List<Map<String, int[]>>[] crazyNested = new List[]{
        List.of(
            Map.of("a", new int[]{1, 2}, "b", new int[]{3}),
            Collections.emptyMap()
        ),
        null,
        Collections.emptyList()
    };

    Map<String, List<Optional<Integer>[]>>[] ultraNested = new Map[]{
        Map.of(
            "key1", List.of(
                new Optional[]{Optional.of(1), Optional.empty()},
                null
            )
        ),
        null
    };

    DeepNestedArrayRecord original = new DeepNestedArrayRecord(
        threeDimInts, crazyNested, ultraNested
    );
    testRoundTrip(original, DeepNestedArrayRecord.class);
  }

  @Test
  void testMixedContainersWithArrays() {
    LOGGER.fine("Testing mixed containers with arrays before and after");

    List<int[]> intArrayList = List.of(
        new int[]{1, 2, 3},
        new int[]{},
        new int[]{42}
    );

    Map<String, Color[]> enumArrayMap = Map.of(
        "primary", new Color[]{Color.RED, Color.GREEN, Color.BLUE},
        "secondary", new Color[]{Color.YELLOW, Color.PURPLE}
    );

    Set<Optional<Double>[]> optionalArraySet = Set.of(
        new Optional[]{Optional.of(1.1), Optional.empty()},
        new Optional[]{Optional.of(2.2)}
    );

    int[][] beforeArray = {{1, 2}, {3, 4, 5}};

    Map<Integer, List<String[]>> afterArray = new HashMap<>();
    afterArray.put(1, Arrays.asList(new String[]{"a", "b"}, new String[]{"c"}));
    afterArray.put(2, Collections.singletonList(new String[]{}));

    MixedContainerRecord original = new MixedContainerRecord(
        intArrayList, enumArrayMap, optionalArraySet, beforeArray, afterArray
    );
    testRoundTrip(original, MixedContainerRecord.class);
  }

  @Test
  void testExtremeArraySizes() {
    LOGGER.fine("Testing extreme array sizes");

    ExtremeSizeRecord original = new ExtremeSizeRecord(
        new int[0],
        new String[]{"only"},
        IntStream.range(0, 1000).mapToDouble(i -> i * 1.1).boxed().toArray(Double[]::new),
        IntStream.range(0, 100)
            .mapToObj(i -> List.of(i, i + 1, i + 2))
            .toArray(List[]::new)
    );

    testRoundTrip(original, ExtremeSizeRecord.class);
  }

  @Test
  void testArraysWithNullElements() {
    LOGGER.fine("Testing arrays with various null patterns");

    NullPatternRecord original = new NullPatternRecord(
        new String[]{null, null, null},
        new Integer[]{null, null, 1, 2, 3},
        new Double[]{1.1, 2.2, 3.3, null, null},
        new Boolean[]{true, null, false, null, true},
        new SimpleRecord[]{
            null,
            new SimpleRecord("sparse", 1),
            null,
            null,
            new SimpleRecord("data", 2),
            null
        }
    );

    testRoundTrip(original, NullPatternRecord.class);
  }

  @Test
  void testStringArrays() {
    LOGGER.fine("Testing string arrays");

    StringArrayRecord original = new StringArrayRecord(
        new String[]{"hello", "world", "", null, "test"},
        new String[][]{
            {"row1col1", "row1col2"},
            null,
            {"", null, "row3col3"}
        },
        new String[0],
        new String[]{"ASCII", "日本語", "العربية", "🎉🌍", "Ñoño"}
    );

    testRoundTrip(original, StringArrayRecord.class);
  }

  @Test
  void testUUIDArrays() {
    LOGGER.fine("Testing UUID arrays");

    UUID[] uuids = {
        UUID.randomUUID(),
        null,
        UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
        UUID.randomUUID()
    };

    UUID[][] nestedUuids = {
        {UUID.randomUUID(), UUID.randomUUID()},
        null,
        new UUID[0],
        {null, UUID.randomUUID(), null}
    };

    UUIDArrayRecord original = new UUIDArrayRecord(uuids, nestedUuids);
    testRoundTrip(original, UUIDArrayRecord.class);
  }

  @Test
  void testPrimitiveEdgeCases() {
    LOGGER.fine("Testing primitive edge cases");

    PrimitiveEdgeCaseRecord original = new PrimitiveEdgeCaseRecord(
        new byte[]{Byte.MIN_VALUE, -1, 0, 1, Byte.MAX_VALUE},
        new char[]{'\u0000', 'A', '中', '\uFFFF'},
        new float[]{Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0.0f, -0.0f, Float.MIN_VALUE, Float.MAX_VALUE},
        new double[]{Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.0, -0.0, Double.MIN_VALUE, Double.MAX_VALUE}
    );

    testRoundTrip(original, PrimitiveEdgeCaseRecord.class);
  }

  @Test
  void testNestedPrimitiveArrays() {
    LOGGER.fine("Testing nested primitive arrays");

    int[][] matrix2d = {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}};
    double[][] doubles2d = {{1.1, 2.2}, {3.3, 4.4, 5.5}, {}};
    byte[][][] bytes3d = {
        {{1, 2}, {3, 4}},
        {{5}, {6, 7, 8}},
        null,
        {{}}
    };

    NestedPrimitiveArrayRecord original = new NestedPrimitiveArrayRecord(matrix2d, doubles2d, bytes3d);
    testRoundTrip(original, NestedPrimitiveArrayRecord.class);
  }

  @Test
  void testMixedPrimitiveObjectArrays() {
    LOGGER.fine("Testing mixed primitive and object arrays");

    MixedPrimitiveObjectArrayRecord original = new MixedPrimitiveObjectArrayRecord(
        new int[]{1, 2, 3},
        new Integer[]{1, null, 3},
        new String[]{"a", null, "c"},
        new SimpleRecord[]{new SimpleRecord("test", 1), null},
        new double[][]{{1.1, 2.2}, {3.3}}
    );

    testRoundTrip(original, MixedPrimitiveObjectArrayRecord.class);
  }

  @Test
  void testEmptyArrays() {
    LOGGER.fine("Testing empty arrays");

    EmptyArraysRecord original = new EmptyArraysRecord(
        new Color[0],
        new SimpleRecord[0],
        new int[0],
        new String[0],
        new List[0]
    );

    testRoundTrip(original, EmptyArraysRecord.class);
  }

  @Test
  void testSingleElementArrays() {
    LOGGER.fine("Testing single element arrays");

    SingleElementArraysRecord original = new SingleElementArraysRecord(
        new Color[]{Color.RED},
        new SimpleRecord[]{new SimpleRecord("single", 42)},
        new int[]{999},
        new String[]{"lonely"}
    );

    testRoundTrip(original, SingleElementArraysRecord.class);
  }

  // Helper method for round-trip testing
  <T extends Serializable> void testRoundTrip(T original, Class<T> clazz) {
    LOGGER.fine(() -> "Testing round-trip for " + clazz.getSimpleName() + ": " + original);

    Pickler<T> pickler = Pickler.forClass(clazz);

    // Calculate size and allocate buffer
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    LOGGER.fine(() -> "Buffer size: " + buffer.capacity() + " bytes");

    // Serialize
    pickler.serialize(buffer, original);
    var position = buffer.position();
    LOGGER.fine(() -> "Serialized size: " + position + " bytes");

    // Prepare for deserialization
    var buf = buffer.flip();

    // Deserialize
    T deserialized = pickler.deserialize(buf);

    // Detailed verification for arrays
    verifyArrayContents(original, deserialized);

    LOGGER.fine(() -> "Round-trip successful for " + clazz.getSimpleName());
  }

  // Helper method to verify array contents in detail
  void verifyArrayContents(Object original, Object deserialized) {
    if (original instanceof BoxedArraysRecord) {
      BoxedArraysRecord o = (BoxedArraysRecord) original;
      BoxedArraysRecord d = (BoxedArraysRecord) deserialized;
      assertArrayEquals(o.integers(), d.integers());
      assertArrayEquals(o.booleans(), d.booleans());
      assertArrayEquals(o.doubles(), d.doubles());
      assertArrayEquals(o.longs(), d.longs());
      assertArrayEquals(o.characters(), d.characters());
    } else if (original instanceof EnumArrayRecord) {
      EnumArrayRecord o = (EnumArrayRecord) original;
      EnumArrayRecord d = (EnumArrayRecord) deserialized;
      assertArrayEquals(o.colors(), d.colors());
      assertDeepArrayEquals(o.colorMatrix(), d.colorMatrix());
    } else if (original instanceof VarintArrayRecord) {
      VarintArrayRecord o = (VarintArrayRecord) original;
      VarintArrayRecord d = (VarintArrayRecord) deserialized;
      assertArrayEquals(o.smallInts(), d.smallInts());
      assertArrayEquals(o.largeInts(), d.largeInts());
      assertArrayEquals(o.mixedLongs(), d.mixedLongs());
    } else if (original instanceof DeepNestedArrayRecord) {
      DeepNestedArrayRecord o = (DeepNestedArrayRecord) original;
      DeepNestedArrayRecord d = (DeepNestedArrayRecord) deserialized;
      assertDeepArrayEquals(o.threeDimInts(), d.threeDimInts());
    } else if (original instanceof StringArrayRecord) {
      StringArrayRecord o = (StringArrayRecord) original;
      StringArrayRecord d = (StringArrayRecord) deserialized;
      assertArrayEquals(o.basicStrings(), d.basicStrings());
      assertDeepArrayEquals(o.nestedStrings(), d.nestedStrings());
      assertArrayEquals(o.emptyStrings(), d.emptyStrings());
      assertArrayEquals(o.unicodeStrings(), d.unicodeStrings());
    } else if (original instanceof UUIDArrayRecord) {
      UUIDArrayRecord o = (UUIDArrayRecord) original;
      UUIDArrayRecord d = (UUIDArrayRecord) deserialized;
      assertArrayEquals(o.uuids(), d.uuids());
      assertDeepArrayEquals(o.nestedUuids(), d.nestedUuids());
    } else if (original instanceof PrimitiveEdgeCaseRecord) {
      PrimitiveEdgeCaseRecord o = (PrimitiveEdgeCaseRecord) original;
      PrimitiveEdgeCaseRecord d = (PrimitiveEdgeCaseRecord) deserialized;
      assertArrayEquals(o.boundaryBytes(), d.boundaryBytes());
      assertArrayEquals(o.unicodeChars(), d.unicodeChars());
      assertArrayEquals(o.specialFloats(), d.specialFloats());
      assertArrayEquals(o.specialDoubles(), d.specialDoubles());
    } else if (original instanceof NestedPrimitiveArrayRecord) {
      NestedPrimitiveArrayRecord o = (NestedPrimitiveArrayRecord) original;
      NestedPrimitiveArrayRecord d = (NestedPrimitiveArrayRecord) deserialized;
      assertDeepArrayEquals(o.matrix2d(), d.matrix2d());
      assertDeepArrayEquals(o.doubles2d(), d.doubles2d());
      assertDeepArrayEquals(o.bytes3d(), d.bytes3d());
    }
  }

  // Helper to assert deep array equality that handles nulls properly
  void assertDeepArrayEquals(Object[][] expected, Object[][] actual) {
    if (expected == null) {
      assertNull(actual);
      return;
    }
    assertNotNull(actual);
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertArrayEquals(expected[i], actual[i]);
    }
  }

  void assertDeepArrayEquals(int[][][] expected, int[][][] actual) {
    if (expected == null) {
      assertNull(actual);
      return;
    }
    assertNotNull(actual);
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        assertNull(actual[i]);
      } else {
        assertNotNull(actual[i]);
        assertEquals(expected[i].length, actual[i].length);
        for (int j = 0; j < expected[i].length; j++) {
          assertArrayEquals(expected[i][j], actual[i][j]);
        }
      }
    }
  }

  void assertDeepArrayEquals(int[][] expected, int[][] actual) {
    if (expected == null) {
      assertNull(actual);
      return;
    }
    assertNotNull(actual);
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertArrayEquals(expected[i], actual[i]);
    }
  }

  void assertDeepArrayEquals(double[][] expected, double[][] actual) {
    if (expected == null) {
      assertNull(actual);
      return;
    }
    assertNotNull(actual);
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertArrayEquals(expected[i], actual[i]);
    }
  }

  void assertDeepArrayEquals(byte[][][] expected, byte[][][] actual) {
    if (expected == null) {
      assertNull(actual);
      return;
    }
    assertNotNull(actual);
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        assertNull(actual[i]);
      } else {
        assertNotNull(actual[i]);
        assertEquals(expected[i].length, actual[i].length);
        for (int j = 0; j < expected[i].length; j++) {
          assertArrayEquals(expected[i][j], actual[i][j]);
        }
      }
    }
  }

  void assertDeepArrayEquals(String[][] expected, String[][] actual) {
    if (expected == null) {
      assertNull(actual);
      return;
    }
    assertNotNull(actual);
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertArrayEquals(expected[i], actual[i]);
    }
  }

  void assertDeepArrayEquals(UUID[][] expected, UUID[][] actual) {
    if (expected == null) {
      assertNull(actual);
      return;
    }
    assertNotNull(actual);
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertArrayEquals(expected[i], actual[i]);
    }
  }

  // Simple record for testing null handling in arrays
  public record SimpleNullArrayRecord(
      String[] stringsWithNull,
      Integer[] integersWithNull
  ) implements Serializable {
  }

  @Test
  void testSimpleNullArrayHandling() {
    LOGGER.fine("Testing simple null handling in arrays");

    SimpleNullArrayRecord original = new SimpleNullArrayRecord(
        new String[]{"hello", null, "world"},
        new Integer[]{1, null, 3}
    );

    testRoundTrip(original, SimpleNullArrayRecord.class);
  }

}

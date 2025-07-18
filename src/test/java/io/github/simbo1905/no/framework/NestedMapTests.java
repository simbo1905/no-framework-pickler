// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.junit.jupiter.api.Assertions.*;

/// Test nested Map support 
public class NestedMapTests {

  // Test records for various Map nesting
  public record SimpleNestedMap(Map<String, Map<Integer, Double>> nestedMap) {
  }

  public record DeepNestedMap(Map<String, Map<Integer, Map<Long, String>>> deepMap) {
  }

  public record MapInList(List<Map<String, Integer>> listOfMaps) {
  }

  public record MapOfLists(Map<String, List<Integer>> mapOfLists) {
  }

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  @Test
  void testSimpleNestedMap() {
    LOGGER.info(() -> "Testing SimpleNestedMap serialization and deserialization testSimpleNestedMap");
    // Map<String, Map<Integer, Double>>
    Map<String, Map<Integer, Double>> data = new HashMap<>();
    Map<Integer, Double> inner1 = new HashMap<>();
    inner1.put(1, 1.5);
    inner1.put(2, 2.5);
    Map<Integer, Double> inner2 = new HashMap<>();
    inner2.put(10, 10.5);
    data.put("first", inner1);
    data.put("second", inner2);

    SimpleNestedMap original = new SimpleNestedMap(data);
    Pickler<SimpleNestedMap> pickler = Pickler.forClass(SimpleNestedMap.class);

    ByteBuffer buffer = ByteBuffer.allocate(1024);
    pickler.serialize(buffer, original);
    buffer.flip();

    SimpleNestedMap deserialized = pickler.deserialize(buffer);

    assertNotNull(deserialized);
    assertEquals(2, deserialized.nestedMap().size());
    assertEquals(1.5, deserialized.nestedMap().get("first").get(1));
    assertEquals(10.5, deserialized.nestedMap().get("second").get(10));
  }

  @Test
  void testDeepNestedMap() {
    LOGGER.info(() -> "Testing DeepNestedMap serialization and deserialization testDeepNestedMap");
    // Map<String, Map<Integer, Map<Long, String>>>
    Map<String, Map<Integer, Map<Long, String>>> data = new HashMap<>();
    Map<Integer, Map<Long, String>> level2 = new HashMap<>();
    Map<Long, String> level3 = new HashMap<>();
    level3.put(100L, "deep value");
    level2.put(1, level3);
    data.put("root", level2);

    DeepNestedMap original = new DeepNestedMap(data);
    Pickler<DeepNestedMap> pickler = Pickler.forClass(DeepNestedMap.class);

    ByteBuffer buffer = ByteBuffer.allocate(1024);
    pickler.serialize(buffer, original);
    buffer.flip();

    DeepNestedMap deserialized = pickler.deserialize(buffer);

    assertNotNull(deserialized);
    assertEquals("deep value", deserialized.deepMap().get("root").get(1).get(100L));
    // ensure that we test inner immutability
    assertThrows(UnsupportedOperationException.class, () -> deserialized.deepMap().get("root").get(1).remove(100L));
  }

  @Test
  void testMapInList() {
    LOGGER.info(() -> "Testing MapInList serialization and deserialization testMapInList");
    // List<Map<String, Integer>>
    List<Map<String, Integer>> data = List.of(
        Map.of("a", 1, "b", 2),
        Map.of("bool", 10, "y", 20)
    );

    MapInList original = new MapInList(data);
    Pickler<MapInList> pickler = Pickler.forClass(MapInList.class);

    ByteBuffer buffer = ByteBuffer.allocate(1024);
    pickler.serialize(buffer, original);
    buffer.flip();

    MapInList deserialized = pickler.deserialize(buffer);

    assertNotNull(deserialized);
    assertEquals(2, deserialized.listOfMaps().size());
    assertEquals(1, deserialized.listOfMaps().get(0).get("a"));
    assertEquals(20, deserialized.listOfMaps().get(1).get("y"));
  }

  @Test
  void testMapOfLists() {
    LOGGER.info(() -> "Testing MapOfLists serialization and deserialization testMapOfLists");
    // Map<String, List<Integer>>
    Map<String, List<Integer>> data = new HashMap<>();
    data.put("evens", List.of(2, 4, 6));
    data.put("odds", List.of(1, 3, 5));

    MapOfLists original = new MapOfLists(data);
    Pickler<MapOfLists> pickler = Pickler.forClass(MapOfLists.class);

    ByteBuffer buffer = ByteBuffer.allocate(1024);
    pickler.serialize(buffer, original);
    buffer.flip();

    MapOfLists deserialized = pickler.deserialize(buffer);

    assertNotNull(deserialized);
    assertEquals(2, deserialized.mapOfLists().size());
    assertEquals(List.of(2, 4, 6), deserialized.mapOfLists().get("evens"));
    assertEquals(List.of(1, 3, 5), deserialized.mapOfLists().get("odds"));
  }

  public record SimpleMap(Map<String, Integer> map) {
  }

  @Test
  void testSimpleMap() {
    LOGGER.info(() -> "Testing SimpleMap serialization and deserialization testSimpleMap");
    // Map<String, Integer>
    Map<String, Integer> data = Map.of("key1", 1, "key2", 2);

    SimpleMap original = new SimpleMap(data);
    Pickler<SimpleMap> pickler = Pickler.forClass(SimpleMap.class);

    ByteBuffer buffer = ByteBuffer.allocate(1024);
    pickler.serialize(buffer, original);
    buffer.flip();

    SimpleMap deserialized = pickler.deserialize(buffer);

    assertNotNull(deserialized);
    assertEquals(2, deserialized.map().size());
    assertEquals(1, deserialized.map().get("key1"));
    assertEquals(2, deserialized.map().get("key2"));
    // test outer immutability
    assertThrows(UnsupportedOperationException.class, () -> deserialized.map().remove("key1"));
  }

  public record OptionalMap(Optional<Map<String, Integer>> optionalMap) {
  }

  @Test
  void testOptionalMap() {
    LOGGER.info(() -> "Testing OptionalMap serialization and deserialization testOptionalMap");
    // Optional<Map<String, Integer>>
    Map<String, Integer> data = Map.of("key1", 1, "key2", 2);
    Optional<Map<String, Integer>> optionalData = Optional.of(data);
    OptionalMap original = new OptionalMap(optionalData);
    Pickler<OptionalMap> pickler = Pickler.forClass(OptionalMap.class);

    ByteBuffer buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    pickler.serialize(buffer, original);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    assertNotNull(deserialized);
    assertTrue(deserialized.optionalMap().isPresent());
    assertEquals(2, deserialized.optionalMap().get().size());
    assertEquals(1, deserialized.optionalMap().get().get("key1"));
    assertEquals(2, deserialized.optionalMap().get().get("key2"));
    // test outer immutability
    assertThrows(UnsupportedOperationException.class, () -> deserialized.optionalMap().get().remove("key1"));
  }

  public record MapOfOptionals(Map<String, Optional<Integer>> mapOfOptionals) {
  }

  @Test
  void testMapOfOptionals() {
    LOGGER.info(() -> "Testing MapOfOptionals serialization and deserialization testMapOfOptionals");
    // Map<String, Optional<Integer>>
    Map<String, Optional<Integer>> data = Map.of(
        "key1", Optional.of(1),
        "key2", Optional.empty()
    );

    MapOfOptionals original = new MapOfOptionals(data);
    Pickler<MapOfOptionals> pickler = Pickler.forClass(MapOfOptionals.class);
    ByteBuffer buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    pickler.serialize(buffer, original);
    buffer.flip();
    MapOfOptionals deserialized = pickler.deserialize(buffer);
    assertNotNull(deserialized);
    assertEquals(2, deserialized.mapOfOptionals().size());
    assertTrue(deserialized.mapOfOptionals().get("key1").isPresent());
    assertEquals(1, deserialized.mapOfOptionals().get("key1").get());
    assertFalse(deserialized.mapOfOptionals().get("key2").isPresent());
    // test outer immutability
    assertThrows(UnsupportedOperationException.class, () -> deserialized.mapOfOptionals().remove("key1"));
  }
}



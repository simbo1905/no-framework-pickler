// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;
import io.github.simbo1905.no.framework.model.*;
import io.github.simbo1905.no.framework.tree.InternalNode;
import io.github.simbo1905.no.framework.tree.LeafNode;
import io.github.simbo1905.no.framework.tree.TreeNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static io.github.simbo1905.no.framework.Pickler.forClass;
import static org.junit.jupiter.api.Assertions.*;

public class RefactorTests {

  public record AllPrimitives(
      boolean boolVal, byte byteVal, short shortVal, char charVal,
      int intVal, long longVal, float floatVal, double doubleVal
  ) implements Serializable {
  }

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  @Test
  void testMyRecordSerialization() {
    // Arrange
    MyRecord record = new MyRecord("Hello", 42);
    Pickler<MyRecord> pickler = Pickler.forClass(MyRecord.class);
    final var buffer = ByteBuffer.allocate(1024);

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip(); // Prepare the buffer for reading
    MyRecord deserializedRecord = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserializedRecord,
        "Deserialized record should equal the original record");
  }

  @Test
  void testDogSerialization() {
    // Arrange
    Dog dog = new Dog("Fido", 2);

    Pickler<Dog> dogPickler = forClass(Dog.class);
    // dogPickler.sizeOf(dog)
    final var dogBuffer = ByteBuffer.allocate(1024);

    // Act
    dogPickler.serialize(dogBuffer, dog);
    final var buf = dogBuffer.flip();
    Dog deserializedDog = dogPickler.deserialize(buf);

    // Assert
    assertEquals(dog, deserializedDog,
        "Deserialized dog should equal the original dog");
  }

  @Test
  void testAnimalListSerialization() {
    // Arrange
    Dog dog = new Dog("Buddy", 3);
    Dog dog2 = new Dog("Fido", 2);
    Eagle eagle = new Eagle(2.1);
    Penguin penguin = new Penguin(true);
    Alicorn alicorn = new Alicorn("Twilight Sparkle",
        new String[]{"elements of harmony", "wings of a pegasus"});

    List<Animal> animals = List.of(dog, dog2, eagle, penguin, alicorn);

    final Pickler<Animal> animalPickler = Pickler.forClass(Animal.class);
    final var animalPackedBuffer = ByteBuffer.allocate(4096);

    // Act - Serialize
    animalPackedBuffer.putInt(animals.size());
    for (Animal animal : animals) {
      animalPickler.serialize(animalPackedBuffer, animal);
    }

    animalPackedBuffer.flip();
    // Deserialize
    int size = animalPackedBuffer.getInt();
    List<Animal> deserializedAnimals = new ArrayList<>(size);
    IntStream.range(0, size).forEach(i -> {
      Animal animal = animalPickler.deserialize(animalPackedBuffer);
      deserializedAnimals.add(animal);
    });

    // Assert
    assertEquals(animals.size(), deserializedAnimals.size(),
        "Deserialized list should have the same number of elements");

    IntStream.range(0, animals.size()).forEach(i -> assertEquals(animals.get(i), deserializedAnimals.get(i),
        "Element at index " + i + " should match after deserialization"));
  }

  @Test
  void testAlicornWithMagicPowersSerialization() {
    // Arrange
    Alicorn alicorn = new Alicorn("Twilight Sparkle",
        new String[]{"elements of harmony", "wings of a pegasus"});

    final Pickler<Animal> animalPickler = Pickler.forClass(Animal.class);
    final var buffer = ByteBuffer.allocate(1024);

    // Act
    animalPickler.serialize(buffer, alicorn);
    buffer.flip();
    Animal deserializedAnimal = animalPickler.deserialize(buffer);

    // Assert
    assertInstanceOf(Alicorn.class, deserializedAnimal, "Deserialized animal should be an instance of Alicorn");
    assertEquals(alicorn, deserializedAnimal,
        "Deserialized alicorn should equal the original alicorn");
  }


  public record LinkedListNode(int value, LinkedListNode next) {
    public LinkedListNode(int value) {
      this(value, null);
    }
  }

  @Test
  void testLinkedNode() {
    // Test linked list serialization
    final var linkedList = new LinkedListNode(1, new LinkedListNode(2, new LinkedListNode(3)));
    Pickler<LinkedListNode> linkedListPickler = Pickler.forClass(LinkedListNode.class);
    final var buffer = ByteBuffer.allocate(1024);
    linkedListPickler.serialize(buffer, linkedList);
    buffer.flip();
    LinkedListNode deserializedLinkedList = linkedListPickler.deserialize(buffer);
    assertEquals(linkedList, deserializedLinkedList,
        "Deserialized linked list should equal the original linked list");
  }

  @Test
  void testTreeNodeSerialization() {
    // Test tree node serialization
    // Arrange
    final var originalRoot = new InternalNode("Root",
        new InternalNode("Branch1", new LeafNode(42), new LeafNode(99)),
        new InternalNode("Branch2", new LeafNode(123), TreeNode.empty()));

    Pickler<TreeNode> treeNodePickler = Pickler.forClass(TreeNode.class);

    // Act - Serialize
    final var buffer = ByteBuffer.allocate(1024);
    treeNodePickler.serialize(buffer, originalRoot);

    // Deserialize
    buffer.flip();
    TreeNode deserializedRoot = treeNodePickler.deserialize(buffer);

    // Assert
    assertTrue(TreeNode.areTreesEqual(originalRoot, deserializedRoot),
        "Deserialized tree should be equal to the original tree");
  }

  // Test record for byte
  public record ByteRecord(byte value) {
  }

  @Test
  void testByteRecordSerialization() {
    // Arrange
    ByteRecord record = new ByteRecord((byte) 127);
    Pickler<ByteRecord> pickler = forClass(ByteRecord.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(record));

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    ByteRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Byte record should be preserved");
  }

  // Test record for short
  public record ShortRecord(short value) {
  }

  @Test
  void testShortRecordSerialization() {
    // Arrange
    ShortRecord record = new ShortRecord((short) 32767);
    Pickler<ShortRecord> pickler = forClass(ShortRecord.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(record));

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    ShortRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Short record should be preserved");
  }

  // Test record for float
  public record FloatRecord(float value) {
  }

  @Test
  void testFloatRecordSerialization() {
    // Arrange
    FloatRecord record = new FloatRecord(3.14159f);
    Pickler<FloatRecord> pickler = forClass(FloatRecord.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(record));

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    FloatRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Float record should be preserved");
  }

  // Test record for double
  public record DoubleRecord(double value) {
  }

  @Test
  void testDoubleRecordSerialization() {
    // Arrange
    DoubleRecord record = new DoubleRecord(Math.PI);
    Pickler<DoubleRecord> pickler = forClass(DoubleRecord.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(record));

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    DoubleRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Double record should be preserved");
  }

  // Test record for Optional<Byte>
  public record OptionalByteRecord(Optional<Byte> value) {
  }

  @Test
  void testOptionalByteRecordSerialization() {
    // Arrange
    OptionalByteRecord record = new OptionalByteRecord(Optional.of((byte) 42));
    Pickler<OptionalByteRecord> pickler = forClass(OptionalByteRecord.class);
    var buffer = ByteBuffer.allocate(pickler.maxSizeOf(record));

    // Act - Present value
    pickler.serialize(buffer, record);
    buffer.flip();
    OptionalByteRecord deserialized = pickler.deserialize(buffer);

    // Assert
    assertEquals(record, deserialized, "Optional<Byte> record with value should be preserved");

    // Test empty optional
    buffer = ByteBuffer.allocate(pickler.maxSizeOf(record));
    OptionalByteRecord emptyRecord = new OptionalByteRecord(Optional.empty());
    pickler.serialize(buffer, emptyRecord);
    buffer.flip();
    OptionalByteRecord deserializedEmpty = pickler.deserialize(buffer);

    assertEquals(emptyRecord, deserializedEmpty, "Empty Optional<Byte> should be preserved");
  }

  // Test record for byte array
  public record ByteArrayRecord(byte[] values) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ByteArrayRecord that = (ByteArrayRecord) o;
      return java.util.Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return java.util.Arrays.hashCode(values);
    }
  }

  @Test
  void testByteArrayRecordSerialization() {
    // Arrange
    ByteArrayRecord record = new ByteArrayRecord(new byte[]{1, 2, 3, 127, -128});
    Pickler<ByteArrayRecord> pickler = forClass(ByteArrayRecord.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(record));

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    ByteArrayRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Byte array record should be preserved");
    assertArrayEquals(record.values(), deserialized.values(), "Byte array contents should match");
  }

  // Test record for short array
  public record ShortArrayRecord(short[] values) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ShortArrayRecord that = (ShortArrayRecord) o;
      return java.util.Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return java.util.Arrays.hashCode(values);
    }
  }

  @Test
  void testShortArrayRecordSerialization() {
    // Arrange
    ShortArrayRecord record = new ShortArrayRecord(new short[]{100, 200, 300, 32767, -32768});
    Pickler<ShortArrayRecord> pickler = forClass(ShortArrayRecord.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(record));

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    ShortArrayRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Short array record should be preserved");
    assertArrayEquals(record.values(), deserialized.values(), "Short array contents should match");
  }

  // Create a record to hold these arrays
  public record PrimitiveArraysRecord(
      byte[] byteArray,
      short[] shortArray,
      char[] charArray,
      int[] intArray,
      long[] longArray,
      float[] floatArray,
      double[] doubleArray
  ) {
  }

  public record DoubleArrayRecord(double[] values) {
  }

  @Test
  void testDoubleArrayRecordSerialization() {
    // Arrange
    DoubleArrayRecord record = new DoubleArrayRecord(new double[]{1.0, 2.5, 3.14, Double.MAX_VALUE, Double.MIN_VALUE});
    Pickler<DoubleArrayRecord> pickler = forClass(DoubleArrayRecord.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(record));
    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    DoubleArrayRecord deserialized = pickler.deserialize(buf);
    // Assert
    assertArrayEquals(record.values(), deserialized.values(), "Double array contents should match");
  }

  @Test
  void testPrimitiveArrays() {
    // Create arrays of all primitive types
    byte[] byteArray = {1, 2, 3, 127, -128};
    short[] shortArray = {1, 2, 3, 32767, -32768};
    char[] charArray = {'a', 'b', 'c', '1', '2'};
    long[] longArray = {1L, 2L, 3L, Long.MAX_VALUE, Long.MIN_VALUE};
    float[] floatArray = {1.0f, 2.5f, 3.14f, Float.MAX_VALUE, Float.MIN_VALUE};
    double[] doubleArray = {1.0, 2.5, 3.14, Double.MAX_VALUE, Double.MIN_VALUE};

    // Create an instance
    int[] intArray = {10, 20, 30, Integer.MAX_VALUE, Integer.MIN_VALUE};

    PrimitiveArraysRecord original = new PrimitiveArraysRecord(
        byteArray,
        shortArray,
        charArray,
        intArray,
        longArray,
        floatArray,
        doubleArray);

    // Get a pickler for the record
    Pickler<PrimitiveArraysRecord> pickler = Pickler.forClass(PrimitiveArraysRecord.class);

    // Calculate size and allocate buffer
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));

    // Serialize
    pickler.serialize(buffer, original);

    var buf = buffer.flip();

    // Deserialize
    PrimitiveArraysRecord deserialized = pickler.deserialize(buf);

    // Verify all arrays
    assertArrayEquals(original.byteArray(), deserialized.byteArray());
    assertArrayEquals(original.shortArray(), deserialized.shortArray());
    assertArrayEquals(original.charArray(), deserialized.charArray());
    assertArrayEquals(original.intArray(), deserialized.intArray());
    assertArrayEquals(original.longArray(), deserialized.longArray());
    assertArrayEquals(original.floatArray(), deserialized.floatArray(), 0.0f);
    assertArrayEquals(original.doubleArray(), deserialized.doubleArray(), 0.0);
  }

  @Test
  void testList() {
    // Here we are deliberately passing in a mutable list to the constructor
    final var original = new ListRecord(new ArrayList<>() {{
      add("A");
      add("B");
    }});

    final var pickler = Pickler.forClass(ListRecord.class);
    final byte[] bytes;
    var buffer = ByteBuffer.allocate(1024);
    final int len = pickler.serialize(buffer, original);
    assert len == buffer.position();
    bytes = new byte[len];
    buffer.flip();
    buffer.get(bytes);

    final ListRecord deserialized = pickler.deserialize((ByteBuffer.wrap(bytes)));

    // Verify the record counts
    assertEquals(original.list().size(), deserialized.list().size());
    assertArrayEquals(original.list().toArray(), deserialized.list().toArray());
    // Verify immutable list by getting the deserialized list and trying to add into the list we expect an exception
    assertThrows(UnsupportedOperationException.class, () -> deserialized.list().removeFirst());
  }

  public record ListRecord(List<String> list) {
    // Use the canonical constructor to make an immutable copy
    public ListRecord {
      list = List.copyOf(list);
    }
  }

  // Model definitions
  public sealed interface Animal permits Mammal, Bird, Alicorn {
  }

  public sealed interface Mammal extends Animal permits Dog, Cat {
  }

  public sealed interface Bird extends Animal permits Eagle, Penguin {
  }

  public record Alicorn(String name, String[] magicPowers) implements Animal, Serializable {
    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      Alicorn alicorn = (Alicorn) o;
      return Objects.equals(name, alicorn.name) && Objects.deepEquals(magicPowers, alicorn.magicPowers);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, Arrays.hashCode(magicPowers));
    }
  }

  public record Dog(String name, int age) implements Mammal, Serializable {
  }

  public record Cat(String name, boolean purrs) implements Mammal, Serializable {
  }

  public record Eagle(double wingspan) implements Bird, Serializable {
  }

  public record Penguin(boolean canSwim) implements Bird, Serializable {
  }

  public record MyRecord(String message, int number) {
    @Override
    public String toString() {
      return "MyRecord{message='" + message + "', number=" + number + '}';
    }
  }

  public record ArrayRecord(String[] values) {
  }

  @Test
  void testArrayRecordSerialization2() {
    // Given an array record
    final var arrayRecord = new ArrayRecord(new String[]{"a", "b"});
    final var arrayPickler = forClass(ArrayRecord.class);

    // When serializing and deserializing
    final var writeBuffer = ByteBuffer.allocate(1024);
    arrayPickler.serialize(writeBuffer, arrayRecord);
    writeBuffer.flip();
    final var deserializedArrayRecord = arrayPickler.deserialize(writeBuffer);

    // Then the records should be equal
    assertArrayEquals(arrayRecord.values(), deserializedArrayRecord.values());
  }

  @Test
  void testDogRoundTripArray() {

    // Given a dog instance
    final var dog = new Dog("Fido", 2);
    final var dog2 = new Dog("Scooby", 56);

    final var dogArray = new Dog[]{dog, dog2};

    final var dogPickler = forClass(Dog.class);

    final byte[] bytes;

    // When serializing and deserializing
    final var writeBuffer = ByteBuffer.allocate(1024);
    writeBuffer.putInt(dogArray.length);
    for (Dog d : dogArray) {
      dogPickler.serialize(writeBuffer, d);
    }
    final var size = writeBuffer.position();
    bytes = new byte[size];
    writeBuffer.flip().get(bytes);

    final Dog[] deserializedDogs;
    final var readBuffer = ByteBuffer.wrap(bytes);
    // Read the size of the array
    final var arraySize = readBuffer.getInt();
    deserializedDogs = new Dog[arraySize];
    // Deserialize each dog in the array
    Arrays.setAll(deserializedDogs, i -> dogPickler.deserialize(readBuffer));

    assertEquals(dogArray.length, deserializedDogs.length);
    // Then the records should be equal
    assertArrayEquals(dogArray, deserializedDogs);
  }

  @Test
  void testSimpleRecordSerialization() {
    // Given a simple record
    final var record = new MyRecord("Hello", 42);
    final var pickler = forClass(MyRecord.class);

    // When serializing and deserializing
    final var writeBuffer = ByteBuffer.allocate(1024);
    pickler.serialize(writeBuffer, record);
    final var readBuffer = (writeBuffer.flip());
    final var deserializedRecord = pickler.deserialize(readBuffer);

    // Then the records should be equal
    assertEquals(record, deserializedRecord);
  }

  @Test
  void testDogRoundTrip() {
    // Given a dog instance
    final var dog = new Dog("Fido", 2);
    final var dogPickler = forClass(Dog.class);

    // When serializing and deserializing
    final var writeBuffer = ByteBuffer.allocate(dogPickler.maxSizeOf(dog));
    dogPickler.serialize(writeBuffer, dog);
    final var readBuffer = writeBuffer.flip();
    final var deserializedDog = dogPickler.deserialize(readBuffer);

    // Then the records should be equal
    assertEquals(dog, deserializedDog);
  }

  @Test
  void testAnimalListRoundTrip() {
    // Given a list of different animal types
    final var dog = new Dog("Buddy", 3);
    final var dog2 = new Dog("Fido", 2);
    final var eagle = new Eagle(2.1);
    final var penguin = new Penguin(true);
    final var alicorn = new Alicorn("Twilight Sparkle",
        new String[]{"elements of harmony", "wings of a pegasus"});
    final var animals = List.of(dog, dog2, eagle, penguin, alicorn);

    // Get pickler for sealed interface
    final var animalPickler = Pickler.forClass(Animal.class);
    final var writeBuffer = ByteBuffer.allocate(4096);

    // When serializing the list
    writeBuffer.putInt(animals.size());
    for (Animal animal : animals) {
      animalPickler.serialize(writeBuffer, animal);
    }
    writeBuffer.flip();
    // And deserializing the list
    final var size = writeBuffer.getInt();
    final var deserializedAnimals = new ArrayList<Animal>(size);
    IntStream.range(0, size).forEach(i -> {
      final var animal = animalPickler.deserialize(writeBuffer);
      deserializedAnimals.add(animal);
    });

    // Then all animals should match
    assertEquals(animals.size(), deserializedAnimals.size());
    IntStream.range(0, animals.size()).forEach(i ->
        assertEquals(animals.get(i), deserializedAnimals.get(i)));
  }


  // Define simple enums for testing
  @SuppressWarnings("unused")
  enum TestColor {
    RED, GREEN, BLUE, YELLOW
  }

  @SuppressWarnings("unused")
  enum TestSize {
    SMALL, MEDIUM, LARGE, EXTRA_LARGE
  }

  // Create a record with enum fields
  public record EnumRecord(TestColor color, TestSize size) {
  }

  @Test
  void testBasicEnum() {


    // Create an instance
    EnumRecord original = new EnumRecord(TestColor.BLUE, TestSize.LARGE);

    // Get a pickler for the record
    Pickler<EnumRecord> pickler = Pickler.forClass(EnumRecord.class);

    // Calculate size and allocate buffer

    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));

    // Serialize
    pickler.serialize(buffer, original);

    var buf = buffer.flip();

    // Deserialize
    EnumRecord deserialized = pickler.deserialize(buf);

    // Verify enum values
    assertEquals(original.color(), deserialized.color());
    assertEquals(original.size(), deserialized.size());
  }

  @Test
  void testNullValues() {
    // Create a record with different null field combinations
    final var original = new NullableFieldsExample(null, 42, null, null);

    Pickler<NullableFieldsExample> pickler = Pickler.forClass(NullableFieldsExample.class);

    // Serialize the record
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    pickler.serialize(buffer, original);
    var buf = buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buf);

    // Verify the deserialized object matches the original
    assertEquals(original, deserialized);
    assertNull(deserialized.stringField());
    assertEquals(Integer.valueOf(42), deserialized.integerField());
    assertNull(deserialized.doubleField());
    assertNull(deserialized.objectField());
  }

  public record OptionalOptionalInt(Optional<Optional<Integer>> value) {
  }

  @Test
  void testOptionalOfOptional() {

    final var original = new OptionalOptionalInt(Optional.of(Optional.of(99)));

    // Get a pickler for the record
    Pickler<OptionalOptionalInt> pickler = Pickler.forClass(OptionalOptionalInt.class);

    // Calculate size and allocate buffer
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));

    // Serialize
    pickler.serialize(buffer, original);

    var buf = buffer.flip();

    // Deserialize
    OptionalOptionalInt deserialized = pickler.deserialize(buf);

    //noinspection OptionalGetWithoutIsPresent
    assertEquals(original.value().get().get(), deserialized.value().get().get());
  }

  public record OptionalExample(Optional<Animal> objectOpt, Optional<Integer> intOpt, Optional<String> stringOpt) {
  }

  @Test
  void testOptionalFields() {
    // Create a record with different Optional scenarios
    final var original = new OptionalExample(
        Optional.empty(),              // Empty optional
        Optional.of(42),               // Optional with Integer
        Optional.of("Hello, World!")   // Optional with String
    );

    Pickler<OptionalExample> pickler = Pickler.forClass(OptionalExample.class);

    // Serialize the record
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    pickler.serialize(buffer, original);
    var buf = buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buf);

    // Verify the deserialized object matches the original
    assertEquals(original, deserialized);
    assertEquals(Optional.empty(), deserialized.objectOpt());
    assertEquals(Optional.of(42), deserialized.intOpt());
    assertEquals(Optional.of("Hello, World!"), deserialized.stringOpt());
  }

  // https://www.perplexity.ai/search/b11ebab9-122c-4841-b4bd-1d55de721ebd
  @SafeVarargs
  @SuppressWarnings("varargs")
  public static <T> Optional<T>[] createOptionalArray(Optional<T>... elements) {
    return elements;
  }

  // Create a record to hold these arrays
  public record OptionalArraysRecord(Optional<String>[] stringOptionals, Optional<Integer>[] intOptionals) {
  }

  @Test
  void testArraysOfOptionals() {
    // Create arrays of Optional values with mixed present/empty values
    Optional<String>[] stringOptionals = createOptionalArray(
        Optional.of("Hello"),
        Optional.empty(),
        Optional.of("World")
    );

    Optional<Integer>[] intOptionals = createOptionalArray(
        Optional.of(42),
        Optional.empty(),
        Optional.of(123),
        Optional.of(456)
    );

    // Create an instance
    OptionalArraysRecord original = new OptionalArraysRecord(stringOptionals, intOptionals);

    // Get a pickler for the record
    Pickler<OptionalArraysRecord> pickler = Pickler.forClass(OptionalArraysRecord.class);

    // Calculate size and allocate buffer

    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));

    // Serialize
    pickler.serialize(buffer, original);

    var buf = buffer.flip();

    // Deserialize
    OptionalArraysRecord deserialized = pickler.deserialize(buf);

    // Verify arrays length
    assertEquals(original.stringOptionals().length, deserialized.stringOptionals().length);
    assertEquals(original.intOptionals().length, deserialized.intOptionals().length);

    // Verify string optionals content
    IntStream.range(0, original.stringOptionals().length)
        .forEach(i -> assertEquals(original.stringOptionals()[i], deserialized.stringOptionals()[i]));

    // Verify integer optionals content
    IntStream.range(0, original.intOptionals().length)
        .forEach(i -> assertEquals(original.intOptionals()[i], deserialized.intOptionals()[i]));
  }

  // Test record for Optional of Array - the flipped case
  public record OptionalOfArrayRecord(
      Optional<String[]> optionalStringArray,
      Optional<Integer[]> optionalIntArray
  ) {
  }

  @Test
  void testOptionalOfArray() {
    // Test Optional<String[]> - an optional containing an array
    Optional<String[]> optionalStringArray = Optional.of(new String[]{"Hello", "World", "!"});
    Optional<Integer[]> optionalIntArray = Optional.empty();

    // Create an instance
    OptionalOfArrayRecord original = new OptionalOfArrayRecord(optionalStringArray, optionalIntArray);

    // Get a pickler for the record
    Pickler<OptionalOfArrayRecord> pickler = Pickler.forClass(OptionalOfArrayRecord.class);

    // Calculate size and allocate buffer
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));

    // Serialize
    pickler.serialize(buffer, original);

    var buf = buffer.flip();

    // Deserialize
    OptionalOfArrayRecord deserialized = pickler.deserialize(buf);

    // Assert structure is preserved
    assertEquals(original.optionalStringArray().isPresent(), deserialized.optionalStringArray().isPresent());
    assertEquals(original.optionalIntArray().isPresent(), deserialized.optionalIntArray().isPresent());

    // Verify string array content when present
    if (original.optionalStringArray().isPresent()) {
      assertArrayEquals(original.optionalStringArray().get(), deserialized.optionalStringArray().get());
    }
  }

  public record Empty() {
  }

  @Test
  void testEmptyRecord() {
    // Create an instance of the empty record
    final var original = new Empty();

    // Get a pickler for the empty record
    Pickler<Empty> pickler = Pickler.forClass(Empty.class);

    // Serialize the empty record
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    pickler.serialize(buffer, original);

    var buf = buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buf);

    // Verify the deserialized object matches the original
    assertEquals(original, deserialized);
  }

  // Create a record with a large number of fields
  public record LargeRecord(
      int field1, int field2, int field3, int field4, int field5,
      int field6, int field7, int field8, int field9, int field10,
      int field11, int field12, int field13, int field14, int field15,
      int field16, int field17, int field18, int field19, int field20,
      int field21, int field22, int field23, int field24, int field25,
      int field26, int field27, int field28, int field29, int field30,
      int field31, int field32, int field33, int field34, int field35,
      int field36, int field37, int field38, int field39, int field40,
      int field41, int field42, int field43, int field44, int field45,
      int field46, int field47, int field48, int field49, int field50,
      int field51, int field52, int field53, int field54, int field55,
      int field56, int field57, int field58, int field59, int field60,
      int field61, int field62, int field63, int field64, int field65,
      int field66, int field67, int field68, int field69, int field70,
      int field71, int field72, int field73, int field74, int field75,
      int field76, int field77, int field78, int field79, int field80,
      int field81, int field82, int field83, int field84, int field85,
      int field86, int field87, int field88, int field89, int field90,
      int field91, int field92, int field93, int field94, int field95,
      int field96, int field97, int field98, int field99, int field100,
      int field101, int field102, int field103, int field104, int field105,
      int field106, int field107, int field108, int field109, int field110,
      int field111, int field112, int field113, int field114, int field115,
      int field116, int field117, int field118, int field119, int field120,
      int field121, int field122, int field123, int field124, int field125,
      int field126, int field127, int field128, int field129
  ) {
  }

  @Test
  void testVeryLargeRecord() {

// Create an instance with dummy values
    LargeRecord original = new LargeRecord(
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
        61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
        81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100,
        101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120,
        121, 122, 123, 124, 125, 126, 127, 128, 129
    );
    final var pickler = Pickler.forClass(LargeRecord.class);
    // Calculate size and allocate buffer

    final var buffer = ByteBuffer.allocate(1024);
    // Serialize
    pickler.serialize(buffer, original);

    var buf = buffer.flip();
    // Deserialize
    LargeRecord deserialized = pickler.deserialize(buf);
    // Verify all fields
    assertEquals(original, deserialized);
  }

  @Test
  void testEmptyArrays() {
    // Create a record with empty arrays
    final var original = new ArrayExample(
        new boolean[0],
        new byte[0],
        new short[0],
        new char[0],
        new int[0],
        new long[0],
        new float[0],
        new double[0],
        new String[0],
        new UUID[0],
        new Person[0]
    );

    Pickler<ArrayExample> pickler = Pickler.forClass(ArrayExample.class);

    // Serialize the record
    final var buffer = ByteBuffer.allocate(1024);
    pickler.serialize(buffer, original);

    var buf = buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buf);

    // Replace direct equality check with component-by-component array comparison
    assertArrayRecordEquals(original, deserialized);

    // Verify empty array handling (redundant with assertArrayRecordEquals but keeping for clarity)
    assertEquals(0, deserialized.intArray().length);
    assertEquals(0, deserialized.stringArray().length);
    assertEquals(0, deserialized.personArray().length);
  }

  @Test
  void testTreeNodeDemo() {
    final var leaf1 = new LeafNode(42);
    final var leaf2 = new LeafNode(99);
    final var leaf3 = new LeafNode(123);
// A lob sided tree
    final var internal1 = new InternalNode("Branch1", leaf1, leaf2);
    final var internal2 = new InternalNode("Branch2", leaf3, TreeNode.empty());
    final var originalRoot = new InternalNode("root", internal1, internal2);

// Get a pickler for the TreeNode sealed interface
    final var pickler = Pickler.forClass(TreeNode.class);

// Calculate max size
    final var maxSize = pickler.maxSizeOf(originalRoot);
    LOGGER.fine(() -> "TreeNode maxSizeOf: " + maxSize);

// Allocate a buffer using calculated max size
    final var buffer = ByteBuffer.allocate(maxSize);

// Serialize only the root node (which should include the entire graph)
    final var actualSize = pickler.serialize(buffer, originalRoot);
    LOGGER.fine(() -> String.format("TreeNode actual wire size: %d, max size: %d, ratio: %.1f%%",
        actualSize, maxSize, (actualSize * 100.0) / maxSize));

// Prepare buffer for reading
    final var buf = buffer.flip();

// Deserialize the root node (which will reconstruct the entire graph)
    final var deserializedRoot = pickler.deserialize(buf);

// See junit tests that Validates the entire tree structure was properly deserialized
    assertTrue(TreeNode.areTreesEqual(originalRoot, deserializedRoot));
  }

  /**
   * Utility method to check array record equality by comparing each component
   * @param expected The expected array record
   * @param actual The actual array record
   */
  void assertArrayRecordEquals(ArrayExample expected, ArrayExample actual) {
    assertArrayEquals(expected.booleanArray(), actual.booleanArray());
    assertArrayEquals(expected.byteArray(), actual.byteArray());
    assertArrayEquals(expected.shortArray(), actual.shortArray());
    assertArrayEquals(expected.charArray(), actual.charArray());
    assertArrayEquals(expected.intArray(), actual.intArray());
    assertArrayEquals(expected.longArray(), actual.longArray());
    assertArrayEquals(expected.floatArray(), actual.floatArray(), 0.0f);
    assertArrayEquals(expected.doubleArray(), actual.doubleArray(), 0.0);
    assertArrayEquals(expected.stringArray(), actual.stringArray());
    assertDeepArrayEquals(expected.personArray(), actual.personArray());
  }

  /**
   * Utility method for deep array comparison that handles objects properly
   * @param expected Expected object array
   * @param actual Actual object array
   */
  void assertDeepArrayEquals(Object[] expected, Object[] actual) {
    assertEquals(expected.length, actual.length);
    java.util.stream.IntStream.range(0, expected.length)
        .forEach(i -> {
          if (expected[i] == null) {
            assertNull(actual[i]);
          } else if (expected[i].getClass().isArray()) {
            // Handle nested arrays
            assertTrue(actual[i].getClass().isArray());
            assertDeepArrayEquals((Object[]) expected[i], (Object[]) actual[i]);
          } else {
            assertEquals(expected[i], actual[i]);
          }
        });
  }

  public record DeepDouble(List<List<Optional<Double>>> deepDoubles) {
  }

  @Test
  void testDeepDoubleList() {
    // Create a record with a deep list of optional doubles
    DeepDouble original = new DeepDouble(
        List.of(
            List.of(Optional.of(1.0), Optional.empty(), Optional.of(3.0)),
            List.of(Optional.empty(), Optional.of(5.0))
        )
    );

    // Get a pickler for the record
    Pickler<DeepDouble> pickler = Pickler.forClass(DeepDouble.class);

    // Calculate size and allocate buffer
    var buffer = ByteBuffer.allocate(1024);

    // Serialize
    pickler.serialize(buffer, original);

    // Flip the buffer to prepare for reading
    var buf = buffer.flip();

    // Deserialize
    DeepDouble deserialized = pickler.deserialize(buf);

    // Verify the deep list structure
    assertEquals(original.deepDoubles().size(), deserialized.deepDoubles().size());
    IntStream.range(0, original.deepDoubles().size())
        .forEach(i -> assertEquals(original.deepDoubles().get(i).size(), deserialized.deepDoubles().get(i).size()));

    // Verify buffer is fully consumed
    assertFalse(buffer.hasRemaining());
  }

  public record NestedListRecord(List<List<String>> nestedList) {
  }

  @Test
  void testNestedLists() {

    // Make the inner lists.
    List<List<String>> nestedList = new ArrayList<>();
    nestedList.add(Arrays.asList("A", "B", "C"));
    nestedList.add(Arrays.asList("D", "E"));

    // The record has mutable inner lists
    NestedListRecord original = new NestedListRecord(nestedList);

    // Get a pickler for the record
    Pickler<NestedListRecord> pickler = Pickler.forClass(NestedListRecord.class);

    // Calculate size and allocate buffer
    var buffer = ByteBuffer.allocate(1024);

    // Serialize
    pickler.serialize(buffer, original);

    // Flip the buffer to prepare for reading
    var buf = buffer.flip();

    // Deserialize
    NestedListRecord deserialized = pickler.deserialize(buf);

    // Verify the nested list structure
    assertEquals(original.nestedList().size(), deserialized.nestedList().size());

    // Verify the inner lists are equal
    IntStream.range(0, original.nestedList().size())
        .forEach(i ->
            assertEquals(original.nestedList().get(i).size(), deserialized.nestedList().get(i).size()));

    // Verify buffer is fully consumed
    assertFalse(buffer.hasRemaining());

    // verify the inner lists are immutable
    assertThrows(UnsupportedOperationException.class, () -> deserialized.nestedList().removeFirst());
  }

  /// Public record containing a UUID field for testing serialization.
  /// This record must be public as required by the Pickler framework.
  public record UserSession(String sessionId, UUID userId, long timestamp) {
  }

  @Test
  void testUuidRoundTripSerialization() {
    // Test UUID serialization round-trip

    // Create a UUID from known values for predictable testing
    final long mostSigBits = 0x550e8400e29b41d4L;
    final long leastSigBits = 0xa716446655440000L;
    final var originalUuid = new UUID(mostSigBits, leastSigBits);

    LOGGER.info(() -> "Created test UUID: " + originalUuid);

    // Create a record containing the UUID
    final var originalRecord = new UserSession("session-123", originalUuid, System.currentTimeMillis());
    LOGGER.info(() -> "Created test record: " + originalRecord);

    // Get a pickler for the record type
    final var pickler = Pickler.forClass(UserSession.class);
    assertNotNull(pickler, "Pickler should not be null");

    // Allocate buffer for writing
    final var writeBuffer = ByteBuffer.allocate(1024);
    // Allocated write buffer

    // Serialize the record
    pickler.serialize(writeBuffer, originalRecord);
    // Serialized record

    // Create read buffer from write buffer
    final var readBuffer = (writeBuffer.flip());

    // Deserialize the record
    final var deserializedRecord = pickler.deserialize(readBuffer);
    assertNotNull(deserializedRecord, "Deserialized record should not be null");
    LOGGER.info(() -> "Deserialized record: " + deserializedRecord);

    // Verify the entire record matches
    assertEquals(originalRecord, deserializedRecord, "Original and deserialized records should be equal");

    // Verify UUID specifically
    assertEquals(originalRecord.userId(), deserializedRecord.userId(), "UUIDs should be equal");
    assertEquals(originalUuid, deserializedRecord.userId(), "Deserialized UUID should match original");

    // Verify UUID components match
    assertEquals(mostSigBits, deserializedRecord.userId().getMostSignificantBits(),
        "Most significant bits should match");
    assertEquals(leastSigBits, deserializedRecord.userId().getLeastSignificantBits(),
        "Least significant bits should match");

    LOGGER.info("UUID round-trip serialization test completed successfully");
  }

  @Test
  void testAllPrimitivesWrite() {
    LOGGER.info("=== Testing AllPrimitives write performance issue ===");

    final var testData = new AllPrimitives(
        true, (byte) 42, (short) 1000, 'A', 123456, 9876543210L, 3.14f, 2.71828
    );

    LOGGER.info(() -> "Test data: " + testData);

    // This should show detailed logging of where the reflection work happens
    final var pickler = forClass(AllPrimitives.class);

    LOGGER.info("=== Starting single write operation ===");

    LOGGER.finer("About to allocate WriteBuffer...");
    final var writeBuffer = ByteBuffer.allocate(256);
    LOGGER.finer("WriteBuffer allocated, about to serialize...");
    pickler.serialize(writeBuffer, testData);
    LOGGER.finer("Serialization complete, about to flip...");
    writeBuffer.flip();
    LOGGER.finer(() -> "Write complete, serialized " + writeBuffer.remaining() + " bytes");

    // Verify round-trip
    final var readBuffer = writeBuffer.duplicate();
    AllPrimitives result = pickler.deserialize(readBuffer);
    assertEquals(testData, result);
    LOGGER.info("Round-trip verification successful");

    LOGGER.info("=== AllPrimitives test complete ===");
  }

  // Map test records
  public record SimpleMap(Map<Double, Double> value) {
  }

  public record VarIntMap(Map<Integer, String> value) {
  }

  public record VarLongMap(Map<Long, Optional<String>> value) {
  }

  public record ManySimpleTypes(
      Map<Boolean, String> boolMap,
      Map<Byte, Integer> byteMap,
      Map<Short, Long> shortMap,
      Map<Character, Float> charMap,
      Map<Integer, Double> intMap,
      Map<Long, String> longMap,
      Map<Float, List<String>> floatMap,
      Map<Double, Optional<Integer>> doubleMap,
      Map<String, String[]> stringMap,
      Map<UUID, List<Optional<String>>> uuidMap
  ) {
  }

  @Test
  void testSimpleDoubleMap() {
    var pickler = Pickler.forClass(SimpleMap.class);

    Map<Double, Double> map = new HashMap<>();
    map.put(1.1, 2.2);
    map.put(3.3, 4.4);
    map.put(5.5, 6.6);

    SimpleMap original = new SimpleMap(map);

    var buffer = ByteBuffer.allocate(1024);
    pickler.serialize(buffer, original);
    buffer.flip();

    SimpleMap deserialized = pickler.deserialize(buffer);
    assertEquals(original, deserialized);
  }

  @Test
  void testVarIntMapSerialization() {
    var pickler = Pickler.forClass(VarIntMap.class);

    Map<Integer, String> map = new HashMap<>();
    map.put(1, "one");
    map.put(100, "hundred");
    map.put(1000, "thousand");

    VarIntMap original = new VarIntMap(map);

    var buffer = ByteBuffer.allocate(1024);
    pickler.serialize(buffer, original);
    buffer.flip();

    VarIntMap deserialized = pickler.deserialize(buffer);
    assertEquals(original, deserialized);
  }

  @Test
  void testVarLongMapWithOptionals() {
    var pickler = Pickler.forClass(VarLongMap.class);

    Map<Long, Optional<String>> map = new HashMap<>();
    map.put(1L, Optional.of("first"));
    map.put(2L, Optional.empty());
    map.put(Long.MAX_VALUE, Optional.of("max"));

    VarLongMap original = new VarLongMap(map);

    var buffer = ByteBuffer.allocate(1024);
    pickler.serialize(buffer, original);
    buffer.flip();

    VarLongMap deserialized = pickler.deserialize(buffer);
    assertEquals(original, deserialized);
  }

  @Test
  void testManySimpleTypesMap() {
    var pickler = Pickler.forClass(ManySimpleTypes.class);

    Map<Boolean, String> boolMap = new HashMap<>();
    boolMap.put(true, "yes");
    boolMap.put(false, "no");

    Map<Byte, Integer> byteMap = new HashMap<>();
    byteMap.put((byte) 1, 100);
    byteMap.put((byte) 2, 200);

    Map<Short, Long> shortMap = new HashMap<>();
    shortMap.put((short) 10, 1000L);

    Map<Character, Float> charMap = new HashMap<>();
    charMap.put('A', 1.1f);
    charMap.put('B', 2.2f);

    Map<Integer, Double> intMap = new HashMap<>();
    intMap.put(42, 3.14);

    Map<Long, String> longMap = new HashMap<>();
    longMap.put(999L, "nine nine nine");

    Map<Float, List<String>> floatMap = new HashMap<>();
    floatMap.put(1.5f, List.of("one", "two", "three"));

    Map<Double, Optional<Integer>> doubleMap = new HashMap<>();
    doubleMap.put(2.5, Optional.of(25));
    doubleMap.put(3.5, Optional.empty());

    Map<String, String[]> stringMap = new HashMap<>();
    stringMap.put("array", new String[]{"a", "b", "c"});

    Map<UUID, List<Optional<String>>> uuidMap = new HashMap<>();
    UUID uuid = UUID.randomUUID();
    uuidMap.put(uuid, List.of(Optional.of("present"), Optional.empty()));

    ManySimpleTypes original = new ManySimpleTypes(
        boolMap, byteMap, shortMap, charMap, intMap,
        longMap, floatMap, doubleMap, stringMap, uuidMap
    );

    var buffer = ByteBuffer.allocate(4096);
    pickler.serialize(buffer, original);
    buffer.flip();

    ManySimpleTypes deserialized = pickler.deserialize(buffer);
    assertEquals(original.boolMap(), deserialized.boolMap());
    assertEquals(original.byteMap(), deserialized.byteMap());
    assertEquals(original.shortMap(), deserialized.shortMap());
    assertEquals(original.charMap(), deserialized.charMap());
    assertEquals(original.intMap(), deserialized.intMap());
    assertEquals(original.longMap(), deserialized.longMap());
    assertEquals(original.floatMap(), deserialized.floatMap());
    assertEquals(original.doubleMap(), deserialized.doubleMap());
    assertArrayEquals(original.stringMap().get("array"), deserialized.stringMap().get("array"));
    assertEquals(original.uuidMap(), deserialized.uuidMap());
  }

  @Test
  void testEmptyMap() {
    var pickler = Pickler.forClass(SimpleMap.class);

    SimpleMap original = new SimpleMap(new HashMap<>());

    var buffer = ByteBuffer.allocate(256);
    pickler.serialize(buffer, original);
    buffer.flip();

    SimpleMap deserialized = pickler.deserialize(buffer);
    assertEquals(original, deserialized);
    assertTrue(deserialized.value().isEmpty());
  }

  // Sealed interface that permits both record and enum - LinkedList example
  public sealed interface Link permits LinkedRecord, LinkEnd {
  }

  public record LinkedRecord(int value, Link next) implements Link {
  }

  public enum LinkEnd implements Link {END}

  @Test
  void testSealedInterfaceWithRecordAndEnum() {
    // Test sealed interface that has both record and enum permits
    var pickler = Pickler.forClass(Link.class);

    // Create a linked list: 1 -> 2 -> END
    Link list = new LinkedRecord(1, new LinkedRecord(2, LinkEnd.END));

    ByteBuffer buffer = ByteBuffer.allocate(1024);
    pickler.serialize(buffer, list);
    buffer.flip();

    Link deserialized = pickler.deserialize(buffer);
    assertEquals(list, deserialized);

    // Also test serializing just the enum
    buffer.clear();
    pickler.serialize(buffer, LinkEnd.END);
    buffer.flip();

    Link deserializedEnd = pickler.deserialize(buffer);
    assertEquals(LinkEnd.END, deserializedEnd);
  }

  // Recursive containers are valid to any depth
  @Test
  void testNestedArrayUsingModelNestedArray() {

    // Create a record with nested array structures of different depths
    NestedArrayExample original = new NestedArrayExample(
        new int[][]{{1, 2}, {3, 4}}, // 2D int array
        new String[][]{{"A", "B"}, {"C", "D"}}, // 2D String array
        new TestEnum[][][]{ // 3D enum array
            {{TestEnum.FIRST, TestEnum.SECOND}, {TestEnum.THIRD}},
            {{TestEnum.SECOND}, {TestEnum.FIRST, TestEnum.THIRD}}
        }
    );

    // Get a pickler for the record
    Pickler<NestedArrayExample> pickler = Pickler.forClass(NestedArrayExample.class);

    // Calculate size and allocate buffer
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));

    // Serialize
    pickler.serialize(buffer, original);

    var buf = buffer.flip();

    // Deserialize
    NestedArrayExample deserialized = pickler.deserialize(buf);

    // Verify the 2D int array
    assertArrayEquals(original.nestedIntArray()[0], deserialized.nestedIntArray()[0]);
    assertArrayEquals(original.nestedIntArray()[1], deserialized.nestedIntArray()[1]);

    // Verify the 2D string array
    assertArrayEquals(original.nestedStringArray()[0], deserialized.nestedStringArray()[0]);
    assertArrayEquals(original.nestedStringArray()[1], deserialized.nestedStringArray()[1]);

    // Verify the 3D enum array
    for (int i = 0; i < original.nested3DEnumArray().length; i++) {
      for (int j = 0; j < original.nested3DEnumArray()[i].length; j++) {
        assertArrayEquals(original.nested3DEnumArray()[i][j], deserialized.nested3DEnumArray()[i][j]);
      }
    }
  }
}

# No Framework Pickler

No Framework Pickler is a tiny serialization library that generates elegant, fast, type-safe serializers for Java
records and sealed interface hierarchies of records — perfect for building elegant message protocols using modern Data
Oriented Programming techniques:

```java
/// Given a sealed interface and its permitted record types using Java's new Data Oriented Programming paradigm:
public sealed interface TreeNode permits TreeNode.InternalNode, TreeNode.LeafNode, TreeNode.TreeEnum {
  record LeafNode(int value) implements TreeNode {
  }

  record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode {
  }

  enum TreeEnum implements TreeNode {EMPTY}

  static TreeNode empty() {
    return TreeEnum.EMPTY;
  }

  /// Sealed interfaces allow for exhaustively pattern matched within switch expressions
  static boolean areTreesEqual(TreeNode l, TreeNode r) {
    return switch (l) {
      case TreeEnum.EMPTY -> r == TreeEnum.EMPTY;
      case LeafNode(var v1) -> r instanceof LeafNode(var v2) && v1 == v2;
      case InternalNode(String n1, TreeNode i1, TreeNode i2) ->
          r instanceof InternalNode(String n2, TreeNode j1, TreeNode j2) &&
              n1.equals(n2) &&
              areTreesEqual(i1, j1) &&
              areTreesEqual(i2, j2);
    };
  }
}

// ByteBuffer for high performance serialization 
ByteBuffer buffer = ByteBuffer.allocate(1024);

// Given a tree of nodes:
final var rootNode = new TreeNode.InternalNode("Root",
    new TreeNode.InternalNode("Branch1", new TreeNode.LeafNode(42), new TreeNode.LeafNode(99)),
    new TreeNode.InternalNode("Branch2", new TreeNode.LeafNode(123), TreeNode.empty()));

// And a type safe pickler for the sealed interface:
Pickler<TreeNode> treeNodePickler = Pickler.forClass(TreeNode.class);

// When we serialize a tree of nodes to a ByteBuffer and load it back out again:
treeNodePickler.

serialize(buffer, rootNode);
buffer.

flip();

TreeNode deserializedRoot = treeNodePickler.deserialize(buffer);

// Then it has elegantly and safely reconstructed the entire tree structure
if(TreeNode.

areTreesEqual(rootNode, deserializedRoot) ){
    System.out.

println("The trees are equal!");
}
```

**No Framework Pickler is Java** where in a single line of code creates a typesafe pickler for a sealed interface
hierarchy of records. There are no annotations. There are no build-time steps. There are no generated data structures
you
need to map to your regular code. There is no special configuration files. It is just Java Records and Sealed
Interfaces.
You get all the convenience that the built-in JDK serialization with none of the downsides.

**No Framework Pickler is fast** as it avoids deep reflection on the hot path by using the JDK's `unreflect` on the
resolved the public constructors and public component accessors of the Java records. This work is one once when the
type-safe pickler is constructed. The
cached [Direct Method Handles](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/invoke/MethodHandleInfo.html#directmh)
are then used to do the actual work. On some workloads it can be 2x faster than standard Java serialization while
creating a binary payload that is 0.5x the size.

**No Framework Pickler is safer** than many alternative approaches including JDK Serialization itself. The pickler
resolves the legal code paths that regular Java code would take when creating the pickler; not when it is reading binary
data. Bad data on the wire will never result in mal-constructed data structures with undefined behaviour.

**No Framework Pickler is expressive** as it works out of the box with nested sealed interfaces of permitted record
types or an outer array of such where the records may contain arbitrarily nested:

- boolean.class
- byte.class
- short.class
- char.class
- int.class
- long.class
- float.class
- double.class
- String.class
- java.util.UUID
- java.time.LocalDate
- java.time.LocalDateTime
- Optional.class
- Record.class
- Map.class
- List.class
- Enum.class
- Arrays of the above

When handling sealed interfaces it is requires all permitted subclasses within the sealed hierarchy must be either
records or sealed interfaces of records. This allows you to use record patterns with type safe exhaustive switch
statements.

**No Framework Pickler backwards compatibility** supports opt-in binary compatibility for adding new components to the
end of your `record` types. You simply provide alternative constructors in your newer code to match the default
constructor of your old code. This is disabled by default.

## Usage

No Framework Pickler enforces that the root type passed to `Pickler.forClass()` must be either:

- A `record` type
- A `sealed interface` that permits mixtures of `record` or `enum` types
- A `sealed interface` that permits mixtures of `record` or `enum` types or nested `sealed interface` of `record` or
  `enum` types to any level

See the Complex Nested Sealed Interfaces example below and also the demo files `PublicApiDemo.java` and
`TreeNodeSealedInterfacePicklerTest.java`.

Java Data Oriented Programming allows for strongly typed and exhaustive switch statements to work with
`sealed interface` types. As library code, No Framework Pickler has no direct access to the complex types and it avoids
reflection on the hot path. This precludes passing outer container types (arrays, List or Map) to the pickler. In
practice this is not a significant limitation as users of the library that have a List, Map or array of a complex sealed
type can simply manually write out the length/size then loop invoking the pickler. To read back, they can manually read
the length/size then loop with an exhaustive switch to handle the concrete types as they wish. See the demo file
`PublicApiDemo.java` for an example.

### Basic Record Serialization

Here the optional `maxSizeOf` will recursively walk any large nested structures or arrays to calculate the exact size of
the buffer needed to hold the serialized data:

```java
/// Define a record using the enum. It **must** be public
public record Month(Season season, String name) {
}

/// Define a simple enum with no fields so no custom constructor. It **must** be public
public enum Season {SPRING, SUMMER, FALL, WINTER}

// Create an instance
var december = new Month(Season.WINTER, "December");

// Get a pickler for the record type containing the enum
Pickler<Month> pickler = Pickler.forClass(Month.class);

// Calculate size and allocate buffer
int size = pickler.maxSizeOf(december);
ByteBuffer buffer = ByteBuffer.allocate(size);

// Serialize to a ByteBuffer
pickler.

serialize(buffer, december);
buffer.

flip();

// Deserialize from the ByteBuffer
Month deserializedMonth = pickler.deserialize(buffer);

// Verify the deserialized enum value
if(!deserializedMonth.

equals(december)){
    throw new

AssertionError("should not be reached");
}
```

### Nested Record Tree

```java
import io.github.simbo1905.no.framework.Pickler;

/// The sealed interface and all permitted record subclasses must be public.
/// The records can be static inner classes or top level classes.
/// Nested sealed interfaces are supported see the Animal example below.
public sealed interface TreeNode permits InternalNode, LeafNode, TreeEnum {
  static TreeNode empty() {
    return TreeEnum.EMPTY;
  }
}

public record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode {
}

public record LeafNode(int value) implements TreeNode {
}

public enum TreeEnum implements TreeNode {
  EMPTY
}

final var leaf1 = new LeafNode(42);
final var leaf2 = new LeafNode(99);
final var leaf3 = new LeafNode(123);
final var internal1 = new InternalNode("Branch1", leaf1, leaf2);
final var internal2 = new InternalNode("Branch2", leaf3, TreeNode.empty());
final var originalRoot = new InternalNode("Root", internal1, internal2);

// Get a pickler for the TreeNode sealed interface
final var pickler = Pickler.forClass(TreeNode.class);

// Calculate buffer size needed for the whole graph reachable from the root node
final var bufferSize = pickler.maxSizeOf(originalRoot);

// Allocate a buffer to hold the entire tree
final var buffer = ByteBuffer.allocate(bufferSize);

// Serialize the root node (which includes the entire graph)
pickler.

serialize(buffer, originalRoot);

// Prepare buffer for reading
buffer.

flip();

// Deserialize the root node (which will reconstruct the entire graph depth first)
final var deserializedRoot = pickler.deserialize(buffer);

// Validates the entire tree structure was properly deserialized
assertTrue(TreeNode.areTreesEqual(originalRoot, deserializedRoot), "Tree structure validation failed");
```

### Returned List Components And Map Components Are Immutable

All deserialized list inside of Records are immutable:

```java
// Create a record with nested lists
record NestedListRecord(List<List<String>> nestedList) {
}

// Make the inner lists.
List<List<String>> nestedList = new ArrayList<>();
nestedList.

add(Arrays.asList("A", "B","C"));
    nestedList.

add(Arrays.asList("D", "E"));

// The record has mutable inner lists
NestedListRecord original = new NestedListRecord(nestedList);

// Get a pickler for the record
Pickler<NestedListRecord> pickler = Pickler.forClass(NestedListRecord.class);

// Calculate size and allocate buffer
int size = pickler.maxSizeOf(original);
ByteBuffer buffer = ByteBuffer.allocate(size);

// Serialize
pickler.

serialize(buffer, original);
buffer.

flip();

// Deserialize
NestedListRecord deserialized = pickler.deserialize(buffer);

// The returned inner lists are immutable
assertThrows(UnsupportedOperationException .class, () ->deserialized.

nestedList().

removeFirst());
```

Maps within records are also returned as immutable:

```java

public record NestedFamilyMapContainer(Person subject, Map<String, Person> relationships) {
}

Person john = new Person("John", 40);
Person michael = new Person("Michael", 65);
Person sarah = new Person("Sarah", 63);

Map<String, Person> familyMap = new HashMap<>();
familyMap.

put("father",michael);
familyMap.

put("mother",sarah);

final var original = new NestedFamilyMapContainer(john, familyMap);

// Get a pickler for the record
final var pickler = Pickler.forClass(NestedFamilyMapContainer.class);
// Calculate size and allocate buffer
int size = pickler.maxSizeOf(original);
ByteBuffer buffer = ByteBuffer.allocate(size);
// Serialize
pickler.

serialize(buffer, original);
// Prepare buffer for reading
buffer.

flip();

// Deserialize
NestedFamilyMapContainer deserialized = pickler.deserialize(buffer);

// The returned inner map are immutable
assertThrows(UnsupportedOperationException .class, () ->deserialized.

relationships().

put("brother",new Person("Tom", 35)));
```

### Complex Nested Sealed Interfaces

This example shows how to serialize and deserialize a heterogeneous array of records that implement a sealed interface.
The records are nested within the sealed interface hierarchy, and the serialization process handles the complexity of
the nested data structures:

```java
// Protocol
sealed interface Animal permits Mammal, Bird, Alicorn {
}

sealed interface Mammal extends Animal permits Dog, Cat {
}

sealed interface Bird extends Animal permits Eagle, Penguin {
}

public record Alicorn(String name, String[] magicPowers) implements Animal {
}

public record Dog(String name, int age) implements Mammal {
}

public record Cat(String name, boolean purrs) implements Mammal {
}

public record Eagle(double wingspan) implements Bird {
}

record Penguin(boolean canSwim) implements Bird {
}

// Create instances of all animal types
static Dog dog = new Dog("Buddy", 3);
static Dog dog2 = new Dog("Fido", 2);
static Animal eagle = new Eagle(2.1);
static Penguin penguin = new Penguin(true);
static Alicorn alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

static List<Animal> animals = List.of(dog, dog2, eagle, penguin, alicorn);
Pickler<Animal> pickler = Pickler.forClass(Animal.class);
final var buffer = ByteBuffer.allocate(1024);

// anyone reading back needs to know how many records to read back
buffer.

putInt(animals.size());

    for(
Animal animal :animals){
    pickler.

serialize(buffer, animal);
}

    buffer.

flip(); // Prepare for reading

// any service reading back needs to know how many records to read back
int size = buffer.getInt();

// Deserialize the correct number of records
List<Animal> deserializedAnimals = new ArrayList<>(size);
IntStream.

range(0,size).

forEach(i ->{
Animal animal = pickler.deserialize(buffer);
    deserializedAnimals.

add(animal);
});
```

### Serialization And Deserialization Of Multiple Records

When serializing multiple records, you need to manually track the count:

```java
// Create a pickler for your type
Pickler<Person> pickler = Pickler.forClass(Person.class);
List<Person> people = List.of(
    new Person("Alice", 30),
    new Person("Bob", 25),
    new Person("Charlie", 40)
);

ByteBuffer buffer = ByteBuffer.allocate(1024);

// Write the count first
buffer.

putInt(people.size());

// Serialize each person
    for(
Person person :people){
    pickler.

serialize(buffer, person);
}

    buffer.

flip(); // Prepare for reading

// Read the count
int count = buffer.getInt();

// Deserialize each person
List<Person> deserializedPeople = new ArrayList<>(count);
for(
int i = 0;
i<count;i++){
    deserializedPeople.

add(pickler.deserialize(buffer));
    }

// Verify the deserialization
assertEquals(people.size(),deserializedPeople.

size());
    for(
int i = 0; i <people.

size();

i++){

assertEquals(people.get(i),deserializedPeople.

get(i));
    }
```

## Security

This library is secure by default by:

1. When you create a Pickler instance the `unreflect` method is used to
   make [Direct Method Handles](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/invoke/MethodHandleInfo.html#directmh)
   for the constructor(s) and component accessors
2. Using the JDK's `ByteBuffer` class to read and write binary data ensures that the bytes are validate by the JDK.
3. Strings are explicitly using UTF8 bytes that are validated by the ByteArray `readUtf8` method.
4. The pickler resolves what are the legal permitted class names of all records within a sealed interface hierarchy at
   when you create the pickler; not when you are deserializing.
5. There is logic for backwards and forwards compatibility of `records`. This is disabled by default so you must
   explicitly enable it.
6. Java has specifically created `record` types to model data transfer objects safely. The backwards and forwards
   compatibility logic matches deserialization components to constructors. The JDK ensures that the canonical
   constructor is always called so that `record` types are always properly initialized.
7. The JDK ensures that nested `record` types can only be constructed bottom-up. This ensures that the deserialization
   logic behaves like normal Java code with no reflection tricks.
8. When `MethodHandle`s are invoked they validate the types and numbers of parameters are correct.

If you instantiate a pickler for a `sealed interface` it ensures that the permitted types of the sealed interface are
all `record` types else nested `sealed interface`s of records. It then builds a map of the validated classNames to the
correct classes. When it reads back the class type strings this is via the `ByteBuffer` method `readUtf8`. It then
checks that string against the map of permitted class names to classes. It then delegates to the pickler for the
concrete record.

## Schema Evolution

No Framework Pickler provides **opt-in** backwards compatibility that is more restrictive than JDK serialization because
we don't write component names to the wire. This keeps the wire format compact but requires careful schema evolution.

### Security by Default

Each record type has an 8-byte signature computed from:

- The record's simple class name
- Each component's name and generic return type signature

For example, `record MyThing(List<Optional<Double>>[] compA)` generates a SHA-256 hash of:
// FIXME this is wrong

```
MyThing!ARRAY!LIST!OPTIONAL!Double!compA
```

In the default `DISABLED` mode, any schema change (reordering, renaming, type changes) causes deserialization to fail
fast with `InvalidClassException`, preventing silent data corruption. This mimics JDK serialization behavior and may
indicate either a developer error or a deliberate targeted attack.

**Security Note**: It is always wise to add encryption-level message confidentiality and message integrity security to
prevent against any form of tampering attack, which is beyond the scope of this documentation.

### Backward Compatibility Modes

## TL;DR

- **DISABLED**: The safe mode which is the default mode. Strictly no backwards compatibility. Fails fast on any schema
  mismatch.
- **ENABLED**: Opt-in mode. Emulates JDK serialization functionality with a risky corner case.
- If you do opt-in **never** reorder existing fields or enum constants in your source file to avoid the risky corner
  case of "swapping" components or enum constants when deserializing.

To opt-in set the system property `no.framework.Pickler.Compatibility`:

```shell
-Dno.framework.Pickler.Compatibility=DISABLED|ENABLED
```

## Compatibility Modes Explained

With No Framework Pickler your java code is the "schema" of your data structures. You can evolve your records and enums
in a way that is safe only by adding new values to the end of the record or enum definitions. This is because we do not
write out field names or enum constant names to the wire.

The two gotcha are only if you opt in to the `ENABLED` mode:

- If you serialize to storage when your code was `record Point(int x, int y)`, then later read it back with
  `record Point(int y, int x)`, there is an undetectable "swapping" of data.
- If you serialize to storage when your code was `enum Status{GOOD,BAD}`, then later read it back with
  `enum Status{GOOD,BAD}`, there is an undetectable "swapping" of constants.

More sophisticated picklers like Protocol Buffers, Apache Avro, and others use external schema-like files. Those require
users to read the manual to understand how to get started. That upfront effort and ongoing complexity gives them more
options to explicitly handle schema evolution. They also allow for fail fast modes on wire format changes. JDK
Serialization as the concept of serialVersionUID mechanism handle schema evolution. We all know how that worked out in
practice.

No Framework Pickler is designed:

- simple and easy to use.
- compact and fast
- safe by default

It therefore use cryptographic hashes to detect schema changes and forbid them by default to be safe by default.

### Example: Adding a Field to a Record

**Original Record (V1):**

```java
package io.github.simbo1905.no.framework.evolution;

public record SimpleRecord(int value) {
}
```

**Evolved Record (V2) - Safe Evolution:**

```java
package io.github.simbo1905.no.framework.evolution;

// Only works with: -Dno.framework.Pickler.Compatibility=ENABLED
public record SimpleRecord(int value, String name, double score) {
  // Backward compatibility constructor for original schema
  public SimpleRecord(int value) {
    this(value, "default", 0.0);
  }
}
```

When deserializing V1 data with V2 code in `ENABLED` mode:

- `value` retains its serialized value (42)
- `name` and `score` use the compatibility constructor's defaults ("default" and 0.0)

**Dangerous Evolution - Never Do This:**

```java
// WRONG: Reordering fields causes silent data corruption!
public record UserInfo(int accessLevel, String name, String department) {
  // This will swap name and accessLevel values!
}
```

### Limitations

- Adding new permitted types to sealed interfaces is not supported in compatibility mode
- The old codebase cannot deserialize new record types it doesn't know about
- You must implement your own routing logic to prevent sending new types to old services

There are unit tests that dynamically compile and class load different versions of records to explicitly test both
backwards and forwards compatibility across three generations. See `SchemaEvolutionTest.java` and
`BackwardsCompatibilityTest.java` for examples of how to write your own tests.

## Fine Compatibility Details

See [BACKWARDS_COMPATIBILITY.md](BACKWARDS_COMPATIBILITY.md) for details.

# Wire Protocol

The wire protocol is designed to be compact and efficient, with a focus on minimizing overhead. It does not use positive markers for user types; instead, it uses 64-bit hash-based signatures for identifying records and enums, ensuring type safety.

### Null Safety

For any non-primitive field, a single byte marker is written to indicate nullability:
- `Byte.MIN_VALUE`: Indicates that the following value is `null`.
- `Byte.MAX_VALUE`: Indicates that the following value is present and not `null`.

This applies to record components, collection elements, and map keys/values. Primitive fields are always written directly as they cannot be `null`.

### Value Types (Primitives, String, UUID, etc.)

Most value types are written directly to the `ByteBuffer` without a type marker to save space.
- **Primitives (`boolean`, `byte`, `short`, `char`, `float`, `double`):** Written using their standard `ByteBuffer` `put` methods.
- **`String`:** Encoded as UTF-8. The length of the byte array is written first as a ZigZag-encoded integer, followed by the bytes themselves.
- **`UUID`:** Written as two `long` values (most significant and least significant bits).
- **`LocalDate`:** Written as three ZigZag-encoded integers (year, month, day).
- **`LocalDateTime`:** Written as seven ZigZag-encoded integers (year, month, day, hour, minute, second, nano).

An exception is made for `int` and `long` to optimize for space. They can be written in one of two ways:
- **Fixed-width:** A marker (`-6` for `INTEGER`, `-8` for `LONG`) is written, followed by the standard 4- or 8-byte value.
- **Variable-width (Varint):** A marker (`-7` for `INTEGER_VAR`, `-9` for `LONG_VAR`) is written, followed by a ZigZag-encoded value. This is used when the value is small enough to fit in fewer bytes than the standard fixed width. The pickler automatically chooses the most compact representation.

### User-Defined Types (`Record` and `Enum`)

User-defined types are not identified by integer markers. Instead, a 64-bit type signature is used.
- **`Record`:** A 64-bit SHA-256 hash of the record's canonical signature (class name + component names and types) is written first. This is followed by the serialized data for each component in declaration order.
- **`Enum`:** A 64-bit SHA-256 hash of the enum's signature (class name + constant names) is written, followed by the ZigZag-encoded ordinal of the enum constant.

This approach ensures that the exact schema of the record or enum is respected during deserialization, preventing data corruption from schema mismatches unless compatibility mode is enabled.

### Container Types

Container types are preceded by a ZigZag-encoded integer marker to identify their type.

| Container / Type | Wire Marker | Description |
|---|---|---|
| `OPTIONAL_EMPTY` | -13 | An empty `Optional` |
| `OPTIONAL_OF` | -14 | An `Optional` with a value |
| `MAP` | -16 | A `Map` |
| `LIST` | -17 | A `List` |
| `ARRAY_RECORD` | -18 | An array of `Record` types |
| `ARRAY_INTERFACE` | -19 | An array of `sealed interface` types |
| `ARRAY_ENUM` | -20 | An array of `Enum` types |
| `ARRAY_BOOLEAN` | -21 | An array of `Boolean` objects |
| `ARRAY_BYTE` | -22 | An array of `Byte` objects |
| `ARRAY_SHORT` | -23 | An array of `Short` objects |
| `ARRAY_CHAR` | -24 | An array of `Character` objects |
| `ARRAY_INT` | -25 | An array of `Integer` objects |
| `ARRAY_LONG` | -26 | An array of `Long` objects |
| `ARRAY_FLOAT` | -27 | An array of `Float` objects |
| `ARRAY_DOUBLE` | -28 | An array of `Double` objects |
| `ARRAY_STRING` | -29 | An array of `String` objects |
| `ARRAY_UUID` | -30 | An array of `UUID` objects |
| `ARRAY_LOCAL_DATE` | -31 | An array of `LocalDate` objects |
| `ARRAY_LOCAL_DATE_TIME` | -32 | An array of `LocalDateTime` objects |
| `ARRAY_ARRAY` | -33 | An array of arrays (`Object[][]`) |
| `ARRAY_LIST` | -34 | An array of `List`s |
| `ARRAY_MAP` | -35 | An array of `Map`s |
| `ARRAY_OPTIONAL` | -36 | An array of `Optional`s |

**Note:** Primitive arrays (`int[]`, `long[]`, etc.) are handled separately with their own markers for optimized encoding (e.g., using `BitSet` for `boolean[]`).

## Acknowledgements

This library uses ZigZag-encoded LEB128-64b9B "varint" functionality written by Gil Tene of Azul Systems. The original
identical code can be found
at [github.com/HdrHistogram/HdrHistogram](https://github.com/HdrHistogram/HdrHistogram/blob/ad76bb512b510a37f6a55fdea32f8f3dd3355771/src/main/java/org/HdrHistogram/ZigZagEncoding.java).
The code was released to the public domain under [CC0 1.0 Universal](http://creativecommons.org/publicdomain/zero/1.0/).

## Logging

You can enable logging using the java.util.logging framework. There are bridges available for the main logging
frameworks
such as for SLF4J and Log4j2. The pickler tests uses the `java.util.logging.ConsoleHandler` to log messages at `INFO`,
`FINE`, `FINER` and `FINEST` level. You can enable this by setting the system property
`java.util.logging.ConsoleHandler.level`. You can run the unit tests with the levels to see the output:

```shell
mvn test -Dtest=RefactorTests,MachinaryTests -Djava.util.logging.ConsoleHandler.level=FINER
```

## License

SPDX-FileCopyrightText: 2025 Simon Massey  
SPDX-License-Identifier: Apache-2.0

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
    new TreeNode.InternalNode("Branch1",
        new TreeNode.LeafNode(42),
        new TreeNode.LeafNode(99)),
    new TreeNode.InternalNode("Branch2",
        new TreeNode.LeafNode(123),
        TreeNode.empty()));

// And a type safe pickler for the sealed interface:
Pickler<TreeNode> treeNodePickler = Pickler.forClass(TreeNode.class);

// When we serialize a tree of nodes to a ByteBuffer and load it back out again:
treeNodePickler.serialize(buffer, rootNode);
buffer.flip();

TreeNode deserializedRoot = treeNodePickler.deserialize(buffer);

// Then it has elegantly and safely reconstructed the entire tree structure
if(TreeNode.areTreesEqual(rootNode, deserializedRoot) ){
    System.out.println("The trees are equal!");
}
```

**No Framework Pickler is Java** where in a single line of code creates a typesafe pickler for a sealed interface
hierarchy of records. There are no annotations. There are no build-time steps. There are no generated data structures
you
need to map to your regular code. There is no special configuration files. It is just Java Records and Sealed
Interfaces.

**No Framework Pickler is fast** as it avoids deep reflection on the hot path by using the JDK's `unreflect` on the
resolved the public constructors and public component accessors of the Java records. This work is one once when the
type-safe pickler is constructed. The
cached [Direct Method Handles](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/invoke/MethodHandleInfo.html#directmh)
are then used to do the actual work. On some workloads it can be 2x faster than standard Java serialization while
creating a binary payload that is 0.5x the size.

**No Framework Pickler is safer** than many alternative approaches including JDK Serialization itself. The pickler
resolves the legal code paths that regular Java code would take when creating the pickler; not when it is reading binary
data. Bad data on the wire will never result in mal-constructed data structures with undefined behaviour.

**No Framework Pickler ❤️ Data Oriented Programming** you get out-of-the-box exhaustive switch statements because no 
codebase should be left behind. Go forth and detructure your the compact binary messages like it is 2025. 

You get all the convenience of the built-in JDK serialization with none of the downsides by simply using Java records as 
your data types. Java enum types are also supported. There is also support for "value" types with a plugin mechanism with 
a demo that maps `java.util.UUID`. The Java language is moving towards allowing you to define custom value types that the 
JVM optimises so if-and-when those appear we can plug those into this library. 

## Project Goals

No Framework Pickler is designed around these core principles:

- **Zero Dependencies**: No external libraries, frameworks, or build-time dependencies
- **Zero Annotations**: Uses Java's type system directly without requiring any annotations
- **Modern Java First**: Built for Java 21+ leveraging records, sealed interfaces, and pattern matching
- **Data-Oriented Programming**: Separates immutable data structures (records) from behavior (static methods)
- **Type Safety**: Compile-time guarantees with no reflection on hot paths
- **Performance**: Fast serialization through direct method handles with compact binary format
- **Simplicity**: Single-line API (`Pickler.forClass()`) with no configuration files or code generation
- **Security by Design**: Only legal Java construction paths, no arbitrary object creation from untrusted data
- **Safety over Speed**: No `Unsafe` hacks and only shallow reflection on the record component public methods 
and public constructors.
- **Safety over Space**: Guard rails where there can be, and a clear “here be dragons” when you opt-in to 
things that can swap data between components  if you record components of the same type in your source code.

## Usage

No Framework Pickler enforces that the root type passed to `Pickler.forClass()` must be either:

- A `record` type
- A `sealed interface` that permits mixtures of `record` or `enum` types
- A `sealed interface` that permits mixtures of `record` or `enum` types or nested `sealed interfaces` of `record` or
  `enum` types to any level

### Basic Record Serialization

```java
/// Define a record using the enum. It **must** be public
public record Month(Season season, String name) {
}

/// Define a simple enum with no fields, so no custom constructor. It **must** be public
public enum Season {SPRING, SUMMER, FALL, WINTER}

// Create an instance
var december = new Month(Season.WINTER, "December");

// Get a pickler for the record type containing the enum
Pickler<Month> pickler = Pickler.forClass(Month.class);

// Calculate size and allocate buffer
int size = pickler.maxSizeOf(december);
ByteBuffer buffer = ByteBuffer.allocate(size);

// Serialize to a ByteBuffer
pickler.serialize(buffer, december);
buffer.flip();

// Deserialize from the ByteBuffer
Month deserializedMonth = pickler.deserialize(buffer);

// Verify the deserialized enum value
if(!deserializedMonth.equals(december)){
  throw newAssertionError("should not be reached");
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

## Schema Evolution

No Framework Pickler provides **opt-in** backwards compatibility that is more restrictive than JDK serialization because
we don't write component names to the wire. This keeps the wire format compact but requires careful schema evolution.

### Compatibility Modes

For details on compatibility modes, see [BACKWARDS_COMPATIBILITY.md](BACKWARDS_COMPATIBILITY.md).

## Create the Pickler with a Custom Handler

Use the `SerdeHandler.forClass(...)` factory method to fluently define the handler directly within the list passed to
the `Pickler`.

```java
// The record instance to be serialized.
final var originalRecord = new EventRecord(UUID.randomUUID(), 404);

// Create the pickler, providing a list of custom handlers.
final var pickler = Pickler.forClass(
    EventRecord.class,
    List.of(
        // Define the handler for UUID fluently inside the list.
        SerdeHandler.forClass(
            UUID.class,
            // Sizer: A UUID is always two longs.
            (obj) -> 2 * Long.BYTES,
            // Writer: Write the most and least significant bits.
            (buffer, obj) -> {
              UUID uuid = (UUID) obj;
              buffer.putLong(uuid.getMostSignificantBits());
              buffer.putLong(uuid.getLeastSignificantBits());
            },
            // Reader: Read the two longs and reconstruct the UUID.
            (buffer) -> {
              long most = buffer.getLong();
              long least = buffer.getLong();
              return new UUID(most, least);
            }
        )
    )
);

// Perform the serialization and deserialization round-trip.
final ByteBuffer buffer = ByteBuffer.allocate(pickler.maxSizeOf(originalRecord));
pickler.serialize(buffer, originalRecord);
buffer.flip();

final var deserializedRecord = pickler.deserialize(buffer);

// Verify the result.
assertEquals(originalRecord, deserializedRecord);
```

By providing the `SerdeHandler`, the framework's type discovery and serialization logic will automatically use your
custom implementation for `UUID` components within any record structure.

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

## Augmented Intelligence (AI) Welcomed

AI as **Augmented Intelligence** is most welcome here. Contributions that enhance *human + agent collaboration* are encouraged. If you want to suggest new agent‑workflows, prompt patterns, or improvements in tooling / validation / introspection, please submit amendments to **AGENTS.md** via standalone PRs. Your ideas make the difference.

When submitting Issues or PRs, please use a "deep research" tool to sanity check your proposal. Then **before** submission un your submission through a strong model with a prompt such as:

> "Please review the AGENTS.md and README.md along with this draft PR/Issue and check that it does not have any gaps and why it might be insufficient, incomplete, lacking a concrete example, duplicating prior issues or PRs, or not be aligned with the project goals or non‑goals."

Please attach the output of that model’s review to your Issue or PR.

## Acknowledgements

This library uses ZigZag-encoded LEB128-64b9B "varint" functionality written by Gil Tene of Azul Systems. The original
identical code can be found
at [github.com/HdrHistogram/HdrHistogram](https://github.com/HdrHistogram/HdrHistogram/blob/ad76bb512b510a37f6a55fdea32f8f3dd3355771/src/main/java/org/HdrHistogram/ZigZagEncoding.java).
ZigZagEncoding was released to the public domain
under [CC0 1.0 Universal](http://creativecommons.org/publicdomain/zero/1.0/).

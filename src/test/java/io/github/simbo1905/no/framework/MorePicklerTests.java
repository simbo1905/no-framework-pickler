// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
package io.github.simbo1905.no.framework;

import io.github.simbo1905.no.framework.animal.*;
import io.github.simbo1905.no.framework.tree.InternalNode;
import io.github.simbo1905.no.framework.tree.LeafNode;
import io.github.simbo1905.no.framework.tree.TreeNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static io.github.simbo1905.no.framework.PicklerTests.stripOutAsciiStrings;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("ALL")
public class MorePicklerTests {
  @BeforeAll
  static void setupLogging() {
    final var logLevel = System.getProperty("java.util.logging.ConsoleHandler.level", "FINER");
    final Level level = Level.parse(logLevel);

    // Configure the primary LOGGER instance
    LOGGER.setLevel(level);
    // Remove all existing handlers to prevent duplicates if this method is called multiple times
    // or if there are handlers configured by default.
    for (Handler handler : LOGGER.getHandlers()) {
      LOGGER.removeHandler(handler);
    }

    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(level);

    // Create and set a custom formatter
    Formatter simpleFormatter = new Formatter() {
      @Override
      public String format(LogRecord record) {
        return record.getMessage() + "\n";
      }
    };
    consoleHandler.setFormatter(simpleFormatter);

    LOGGER.addHandler(consoleHandler);

    // Ensure parent handlers are not used to prevent duplicate logging from higher-level loggers
    LOGGER.setUseParentHandlers(false);

    LOGGER.info("Logging initialized at level: " + level);
  }

  /// Tests serialization and deserialization of all animal types in a single buffer
  @Test
  void allAnimalsInBuffer() {
    // Create instances of all animal types
    final var dog = new Dog("Buddy", 3);
    final var cat = new Cat("Whiskers", true);
    final var eagle = new Eagle(2.1);
    final var penguin = new Penguin(true);
    final var alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

    // Create an array of all animals
    final var originalAnimals = new Animal[]{dog, cat, eagle, penguin, alicorn};

    // Get a pickler for the Animal sealed interface
    final var pickler = Pickler.forSealedInterface(Animal.class);

    // Allocate a single buffer to hold all animals
    final var buffer = pickler.allocateSufficient(originalAnimals);

    // Serialize all animals into the buffer using streams
    Arrays.stream(originalAnimals).forEach(animal -> pickler.serialize(buffer, animal));

    assertFalse(buffer.hasRemaining());

    // Prepare buffer for reading
    var buf = ReadBuffer.wrap(buffer.flip());

    // Deserialize all animals from the buffer
    final var deserializedAnimals = new Animal[originalAnimals.length];
    Arrays.setAll(deserializedAnimals, ignored -> pickler.deserialize(buf));

    // Verify all animals were correctly deserialized
    assertEquals(originalAnimals.length, deserializedAnimals.length);

    // Check each animal pair using streams and switch expressions
    java.util.stream.IntStream.range(0, originalAnimals.length).forEach(i -> {
      final var original = originalAnimals[i];
      final var deserialized = deserializedAnimals[i];

      // Check type equality
      assertEquals(original.getClass(), deserialized.getClass());

      // Type-specific checks using switch expression
      switch (original) {
        case Dog origDog -> {
          final var deserDog = (Dog) deserialized;
          assertEquals(origDog.name(), deserDog.name());
          assertEquals(origDog.age(), deserDog.age());
          assertEquals(original, deserialized);
        }
        case Cat origCat -> {
          final var deserCat = (Cat) deserialized;
          assertEquals(origCat.name(), deserCat.name());
          assertEquals(origCat.purrs(), deserCat.purrs());
          assertEquals(original, deserialized);
        }
        case Eagle origEagle -> {
          final var deserEagle = (Eagle) deserialized;
          assertEquals(origEagle.wingspan(), deserEagle.wingspan());
          assertEquals(original, deserialized);
        }
        case Penguin origPenguin -> {
          final var deserPenguin = (Penguin) deserialized;
          assertEquals(origPenguin.canSwim(), deserPenguin.canSwim());
          assertEquals(original, deserialized);
        }
        case Alicorn origAlicorn -> {
          final var deserAlicorn = (Alicorn) deserialized;
          assertEquals(origAlicorn.name(), deserAlicorn.name());
          assertArrayEquals(origAlicorn.magicPowers(), deserAlicorn.magicPowers());
          // Skip equality check for Alicorn due to array field
        }
      }
    });

    // Verify buffer is fully consumed
    assertEquals(0, buffer.remaining(), "Buffer should be fully consumed");
  }

  /// Tests serialization and deserialization of all tree nodes individually
  @Test
  void treeStructurePickling() {
    // Get the standard tree nodes
    final var leaf1 = new LeafNode(42);
    final var leaf2 = new LeafNode(99);
    final var leaf3 = new LeafNode(123);

    // Internal nodes
    final var internal1 = new InternalNode("Branch1", leaf1, leaf2);
    final var internal2 = new InternalNode("Branch2", leaf3, null);

    // Root node
    final var root = new InternalNode("root", internal1, internal2);

    // Create an array of all tree nodes (excluding null values)
//    final var originalNodes = new TreeNode[]{root, internal1, internal2, leaf1, leaf2, leaf3};
    final var originalNodes = new TreeNode[]{root, internal1};

    // Get a pickler for the TreeNode sealed interface
    final var pickler = Pickler.forSealedInterface(TreeNode.class);

    // Allocate a single buffer to hold all animals
    final var buffer = pickler.allocateSufficient(originalNodes);

    // Serialize all nodes into the buffer
    Arrays.stream(originalNodes).forEach(node -> {
      LOGGER.fine(() -> "Serializing node: " + node + " with buffer remaining: " + buffer.remaining());
      pickler.serialize(buffer, node);
    });

    assertFalse(buffer.hasRemaining());

    // Prepare buffer for reading
    var buf = ReadBuffer.wrap(buffer.flip());

    // Deserialize all nodes from the buffer
    final var deserializedNodes = new TreeNode[originalNodes.length];
    Arrays.setAll(deserializedNodes, ignored -> pickler.deserialize(buf));

    // Verify all nodes were correctly deserialized
    assertEquals(originalNodes.length, deserializedNodes.length);

    // Check each node pair
    java.util.stream.IntStream.range(0, originalNodes.length).forEach(i -> {
      final var original = originalNodes[i];
      final var deserialized = deserializedNodes[i];

      // Check type equality
      assertEquals(original.getClass(), deserialized.getClass());

      // Type-specific checks using pattern matching
      switch (original) {
        case InternalNode origInternal -> {
          final var deserInternal = (InternalNode) deserialized;
          assertEquals(origInternal.name(), deserInternal.name());

          // Check children types
          if (origInternal.left() != null) {
            assertNotNull(deserInternal.left());
            assertEquals(origInternal.left().getClass(), deserInternal.left().getClass());
          } else {
            assertNull(deserInternal.left());
          }

          if (origInternal.right() != null) {
            assertNotNull(deserInternal.right());
            assertEquals(origInternal.right().getClass(), deserInternal.right().getClass());
          } else {
            assertNull(deserInternal.right());
          }
        }
        case LeafNode origLeaf -> {
          final var deserLeaf = (LeafNode) deserialized;
          assertEquals(origLeaf.value(), deserLeaf.value());
        }
      }
    });

    // Verify buffer is fully consumed
    assertEquals(0, buffer.remaining(), "Buffer should be fully consumed");
  }

  public sealed interface Chained permits Link {
  }

  record Link(Link next) implements Chained {
  }

  @Test
  void testClassNameCompression() {
    // Get a pickler for the Chained sealed interface
    final var pickler = Pickler.forRecord(Link.class);

    // Create a chain of links
    final var link0 = new Link(null);
    final var link1 = new Link(link0);
    final var link2 = new Link(link1);

    // Calculate buffer size needed for the entire chain
    // Allocate a buffer to hold the entire chain
    var buffer = pickler.allocateSufficient(link2);
    // Serialize the entire chain
    pickler.serialize(buffer, link2);
    assertFalse(buffer.hasRemaining());
    // Prepare buffer for reading
    var buf = buffer.flip();

    // Get the bytes from the buffer
    final var bytes = buf.array();
    StringBuilder escapedSearchString = stripOutAsciiStrings(bytes);
    Matcher matcher = Pattern.compile(Link.class.getName().replace("$", "\\$")).matcher(escapedSearchString.toString());
    int count = 0;
    while (matcher.find()) {
      count++;
    }
    assertEquals(1, count);
    final var buf2 = ReadBuffer.wrap(ByteBuffer.wrap(bytes));

    // Deserialize the entire chain
    final var deserializedChain = pickler.deserialize(buf2);
    assertNotNull(deserializedChain);
    var next = deserializedChain.next();
    assertNotNull(next);
    next = next.next();
    assertNotNull(next);
    next = next.next();
    assertNull(next);
  }

  @Test
  void testClassNameCompressionSealedTrait() {
    // Get a pickler for the Chained sealed interface
    final var pickler = Pickler.forSealedInterface(Chained.class);

    // Create a chain of links
    final Chained chained;
    {
      // Create a chain of four links
      final var link1 = new Link(null);
      final var link2 = new Link(link1);
      final var link3 = new Link(link2);
      final var link4 = new Link(link3);
      chained = link4;
    }

    // Allocate a buffer to hold the entire chain
    var buffer = pickler.allocateSufficient(chained);
    // Serialize the entire chain
    pickler.serialize(buffer, chained);
    assertFalse(buffer.hasRemaining());
    // Prepare buffer for reading
    var buf = buffer.flip();

    // Get the bytes from the buffer
    final var bytes = buf.array();
    StringBuilder escapedSearchString = stripOutAsciiStrings(bytes);
    final String n1 = Chained.class.getName();
    final String n2 = Link.class.getName();
    final String shortestCommonPart = IntStream.range(0, Math.min(n1.length(), n2.length()))
        .mapToObj(i -> n1.substring(0, i + 1))
        .takeWhile(prefix -> n2.startsWith(prefix))
        .reduce((first, second) -> second)
        .orElse("");
    Matcher matcher = Pattern.compile(shortestCommonPart.replace("$", "\\$")).matcher(escapedSearchString.toString());
    int count = 0;
    while (matcher.find()) {
      count++;
    }
    // we expect 1 matches because the outer trait has to write out that the inner is a link
    assertEquals(1, count);
    // make a fresh buffer to check that all the links are deserialized
    final var buf2 = ReadBuffer.wrap(ByteBuffer.wrap(bytes));

    // Deserialize the entire chain
    final var deserializedChain = (Link) pickler.deserialize(buf2);
    assertNotNull(deserializedChain);
    var next = deserializedChain.next();
    assertNotNull(next);
    next = next.next();
    assertNotNull(next);
    next = next.next();
    assertNotNull(next);
    next = next.next();
    assertNull(next);
  }

  /// Tests serialization and deserialization of the entire tree through just the root node
  @Test
  void treeGraphSerialization() {
    // Get the standard tree nodes
    final var leaf1 = new LeafNode(42);
    final var leaf2 = new LeafNode(99);
    final var leaf3 = new LeafNode(123);

    // Internal nodes
    final var internal1 = new InternalNode("Branch1", leaf1, leaf2);
    final var internal2 = new InternalNode("Branch2", leaf3, null);

    // Root node
    final var root = new InternalNode("root", internal1, internal2);

    // Create an array of all tree nodes (excluding null values)
    final var originalNodes = new TreeNode[]{root, internal1, internal2, leaf1, leaf2, leaf3};
    final var originalRoot = originalNodes[0];
    
    // Get a pickler for the TreeNode sealed interface
    final var pickler = Pickler.forSealedInterface(TreeNode.class);
    
    // Allocate a buffer to hold just the root node
    var buffer = pickler.allocateSufficient(originalRoot);
    
    // Serialize only the root node (which should include the entire graph)
    pickler.serialize(buffer, originalRoot);

    assertFalse(buffer.hasRemaining());

    // Prepare buffer for reading
    var buf = buffer.flip();

    final var bytes = buf.array();
    StringBuilder escapedSearchString = stripOutAsciiStrings(bytes);

    Matcher matcher = Pattern.compile(LeafNode.class.getName()).matcher(escapedSearchString.toString());

    int count = 0;
    while (matcher.find()) {
      count++;
    }

    assertEquals(1, count);

    final var buf2 = ReadBuffer.wrap(ByteBuffer.wrap(bytes));

    // Deserialize the root node (which should reconstruct the entire graph)
    final var deserializedRoot = pickler.deserialize(buf2);
    
    // Validate the entire tree structure was properly deserialized
    assertTrue(TreeNode.areTreesEqual(originalRoot, deserializedRoot), "Tree structure validation failed");
  }

  // Record for testing Unicode content
  @SuppressWarnings("NonAsciiCharacters")
  public record UnicodeData(
      String ಢ_ಢ,
      String[] tags,
      java.util.Optional<String> note
  ) {
  }

  // Record using valid Unicode characters in name
  @SuppressWarnings("NonAsciiCharacters")
  public record データ_αβγ_КПД(
      String デ,
      String[] タ,
      java.util.Optional<String> Д
  ) {
  }

  @Test
  void testUnicodeContentRoundTrip() {
    var pickler = Pickler.forRecord(データ_αβγ_КПД.class);

    var original = new データ_αβγ_КПД(
        "Rainbow ✨",
        new String[]{"🌈", "⭐", "🌟"},
        java.util.Optional.of("Magic 🦄")
    );

    var buffer = pickler.allocateSufficient(original);
    pickler.serialize(buffer, original);
    var buf = ReadBuffer.wrap(buffer.flip());

    var deserialized = pickler.deserialize(buf);

    assertEquals(original.デ(), deserialized.デ());
    assertArrayEquals(original.タ(), deserialized.タ());
    assertEquals(original.Д(), deserialized.Д());
    assertEquals(0, buffer.remaining(), "Buffer should be fully consumed");
  }
}

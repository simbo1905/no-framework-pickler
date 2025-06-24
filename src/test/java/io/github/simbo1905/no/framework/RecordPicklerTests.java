// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.*;

import java.nio.ByteBuffer;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

public class RecordPicklerTests {

  public record SimpleLinkedList(int value, SimpleLinkedList next) {
    public SimpleLinkedList(int value) {
      this(value, null);
    }
  }

  public record InnerRecordNullEndLinkedList(Inner inner, InnerRecordNullEndLinkedList next) {
    public record Inner(int value) {
    }
  }

  // inner interface of nested records and empty record end
  public sealed interface LinkListEmptyEnd permits LinkListEmptyEnd.LinkedRecord, LinkListEmptyEnd.LinkEnd {

    // record implementing interface
    record LinkedRecord(Boxed value, LinkListEmptyEnd next) implements LinkListEmptyEnd {
    }

    // enum implementing interface
    record LinkEnd() implements LinkListEmptyEnd {
    }

    // inner record
    record Boxed(int value) {
    }
  }

  // A linked list with two nodes to test with
  static LinkListEmptyEnd linkedListTwoNodes =
      new LinkListEmptyEnd.LinkedRecord(new LinkListEmptyEnd.Boxed(1),
          new LinkListEmptyEnd.LinkedRecord(new LinkListEmptyEnd.Boxed(2),
              new LinkListEmptyEnd.LinkEnd()));

  @BeforeAll
  static void setupLogging() {
    io.github.simbo1905.LoggingControl.setupCleanLogging();
  }

  @BeforeEach
  void setUp() {
    LOGGER.fine(() -> "Starting RecordPicklerTests test");
  }

  @AfterEach
  void tearDown() {
    LOGGER.fine(() -> "Finished RecordPicklerTests test");
  }

  @Test
  @DisplayName("Test simple record value round trips")
  void testBoxedRoundTrip() {
    // Create a pickler for the Boxed record
    Pickler<LinkListEmptyEnd.Boxed> pickler = Pickler.forClass(LinkListEmptyEnd.Boxed.class);

    // Create an instance of Boxed
    LinkListEmptyEnd.Boxed original = new LinkListEmptyEnd.Boxed(42);

    // Serialize the original record to a ByteBuffer
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    int size = pickler.serialize(buffer, original);
    buffer.flip(); // Prepare for reading

    // Deserialize the record from the ByteBuffer
    LinkListEmptyEnd.Boxed deserialized = pickler.deserialize(buffer);

    // Assert that the original and deserialized records are equal
    assertThat(deserialized).isEqualTo(original);

    // check the size is less than the maxSizeOf
    int maxSizeOf = pickler.maxSizeOf(original);
    assertThat(size).isLessThanOrEqualTo(maxSizeOf);
  }

  @Test
  @DisplayName("Test linked record null end serialization and deserialization")
  void testSimpleLinkedListRoundTrip() {
    // Create a linked list with two nodes
    SimpleLinkedList linkedList = new SimpleLinkedList(1, new SimpleLinkedList(2));

    // Create a pickler for the SimpleLikedList record
    Pickler<SimpleLinkedList> pickler = Pickler.forClass(SimpleLinkedList.class);

    // Serialize the linked list to a ByteBuffer
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    int size = pickler.serialize(buffer, linkedList);
    buffer.flip(); // Prepare for reading

    // Deserialize the linked list from the ByteBuffer
    SimpleLinkedList deserialized = pickler.deserialize(buffer);

    // Assert that the original and deserialized linked lists are equal
    assertThat(deserialized).isEqualTo(linkedList);

    // check the size is less than the maxSizeOf
    int maxSizeOf = pickler.maxSizeOf(linkedList);
    assertThat(size).isLessThanOrEqualTo(maxSizeOf);
  }

  @Test
  @DisplayName("Test linked nested record interface with empty end")
  void testLinkedNestedRecordEmptyEnd() {
    // Create a pickler for the Link interface
    LOGGER.fine(() -> "-------------\nPickler for Link creation:");
    Pickler<LinkListEmptyEnd> pickler = Pickler.forClass(LinkListEmptyEnd.class);

    // Serialize the linked list with two nodes
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    LOGGER.fine(() -> "-------------\nserialize");
    int size = pickler.serialize(buffer, linkedListTwoNodes);
    buffer.flip(); // Prepare for reading

    LOGGER.fine(() -> "-------------\ndeserialize");
    // Deserialize the linked list
    LinkListEmptyEnd deserialized = pickler.deserialize(buffer);

    // Assert that the deserialized structure matches the original
    assertThat(deserialized).isEqualTo(linkedListTwoNodes);

    // check the size is less than the maxSizeOf
    int maxSizeOf = pickler.maxSizeOf(linkedListTwoNodes);
    assertThat(size).isLessThanOrEqualTo(maxSizeOf);
  }

  @Test
  @DisplayName("Test nested record linked list null end")
  public void testLinedListNullEnd() {
    // Create a pickler for the LinkedListNoInterface record
    Pickler<InnerRecordNullEndLinkedList> pickler = Pickler.forClass(InnerRecordNullEndLinkedList.class);

    // Create an instance of LinkedListNoInterface
    InnerRecordNullEndLinkedList original = new InnerRecordNullEndLinkedList(
        new InnerRecordNullEndLinkedList.Inner(1),
        new InnerRecordNullEndLinkedList(new InnerRecordNullEndLinkedList.Inner(2), null));

    // Serialize the original record to a ByteBuffer
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    int size = pickler.serialize(buffer, original);
    buffer.flip(); // Prepare for reading

    // Deserialize the record from the ByteBuffer
    InnerRecordNullEndLinkedList deserialized = pickler.deserialize(buffer);

    // Assert that the original and deserialized records are equal
    assertThat(deserialized).isEqualTo(original);

    // check the size is less than the maxSizeOf
    int maxSizeOf = pickler.maxSizeOf(original);
    assertThat(size).isLessThanOrEqualTo(maxSizeOf);
  }

  // inner interface of nested records and empty record end
  public sealed interface LinkListEnumEnd permits LinkListEnumEnd.LinkedRecord, LinkListEnumEnd.LinkEnd {

    record LinkedRecord(int value, LinkListEnumEnd next) implements LinkListEnumEnd {
    }

    // enum implementing interface
    enum LinkEnd implements LinkListEnumEnd {
      INSTANCE
    }
  }

  final LinkListEnumEnd linkedListEnumTwoNodes =
      new LinkListEnumEnd.LinkedRecord(1,
          new LinkListEnumEnd.LinkedRecord(2, LinkListEnumEnd.LinkEnd.INSTANCE));

  @Test
  @DisplayName("Test linked record enum end serialization and deserialization")
  void testLinkedRecordEnumEndRoundTrip() {
    // Create a pickler for the LinkListEnumEnd interface
    Pickler<LinkListEnumEnd> pickler = Pickler.forClass(LinkListEnumEnd.class);

    // Serialize the linked list with two nodes
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    int size = pickler.serialize(buffer, linkedListEnumTwoNodes);
    buffer.flip(); // Prepare for reading

    // Deserialize the linked list
    LinkListEnumEnd deserialized = pickler.deserialize(buffer);

    // Assert that the deserialized structure matches the original
    assertThat(deserialized).isEqualTo(linkedListEnumTwoNodes);

    // check the size is less than the maxSizeOf
    int maxSizeOf = pickler.maxSizeOf(linkedListEnumTwoNodes);
    assertThat(size).isLessThanOrEqualTo(maxSizeOf);
  }

}


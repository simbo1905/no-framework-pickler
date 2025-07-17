// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905;


import io.github.simbo1905.no.framework.Pickler;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LinkedListNodeDemo {
  public record LinkedListNode(int value, LinkedListNode next) {
    public LinkedListNode(int value) {
      this(value, null);
    }
  }

  public static void main(String[] args) {
    // Test linked list serialization
    final var linkedList = new LinkedListNode(1, new LinkedListNode(2, new LinkedListNode(3)));
    Pickler<LinkedListNode> linkedListPickler = Pickler.forClass(LinkedListNode.class);
    final var buffer = ByteBuffer.allocate(1024);
    linkedListPickler.serialize(buffer, linkedList);
    buffer.flip();
    LinkedListNode deserializedLinkedList = linkedListPickler.deserialize(buffer);
    assertEquals(linkedList, deserializedLinkedList,
        "Deserialized linked list should equal the original linked list");
    System.out.println("Linked list serialization and deserialization works as expected.");
    System.out.println("written " + linkedList);
    System.out.println("read    " + deserializedLinkedList);
  }
}

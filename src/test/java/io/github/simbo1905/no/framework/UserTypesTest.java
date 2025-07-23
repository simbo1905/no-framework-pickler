// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class UserTypesTest {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  // 6. UserTypesTest
  // Purpose: Test enums, records, interfaces, and recursive structures
  @SuppressWarnings("unused")
  public enum Priority {LOW, MEDIUM, HIGH}

  public record ComplexRecord(String name, int value) {
  }

  public sealed interface Animal permits Dog, Cat {
  }

  public record Dog(String name) implements Animal {
  }

  public record Cat(String name) implements Animal {
  }

  public record EnumTypesRecord(ArrayTypesTest.Color color, Priority priority) {
  }

  public record RecordTypesRecord(ArrayTypesTest.SimpleRecord simple, ComplexRecord complex) {
  }

  public record InterfaceTypesRecord(Animal animal) {
  }

  // Recursive record for tree structure
  public record TreeNode(String value, TreeNode left, TreeNode right) {
  }

  public record RecursiveTypesRecord(TreeNode tree) {
  }


  @Test
  void testEnumTypes() {
    final var record = new EnumTypesRecord(ArrayTypesTest.Color.GREEN, Priority.HIGH);
    final var pickler = Pickler.forClass(EnumTypesRecord.class);
    final var maxSizeOfRecord = pickler.maxSizeOf(record);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
    final int bytesWritten = pickler.serialize(buffer, record);
    assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(record);
  }

  @Test
  void testRecordTypes() {
    final var record = new RecordTypesRecord(new ArrayTypesTest.SimpleRecord(123), new ComplexRecord("test", 456));
    final var pickler = Pickler.forClass(RecordTypesRecord.class);
    final var maxSizeOfRecord = pickler.maxSizeOf(record);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
    final int bytesWritten = pickler.serialize(buffer, record);
    assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(record);
  }

  @Test
  void testInterfaceTypes() {
    final var pickler = Pickler.forClass(InterfaceTypesRecord.class);

    final var record1 = new InterfaceTypesRecord(new Dog("Fido"));
    final var maxSizeOfRecord1 = pickler.maxSizeOf(record1);
    final ByteBuffer buffer1 = ByteBuffer.allocate(maxSizeOfRecord1 + 64);
    final int bytesWritten1 = pickler.serialize(buffer1, record1);
    assertThat(bytesWritten1).isLessThanOrEqualTo(maxSizeOfRecord1);
    buffer1.flip();
    final var deserialized1 = pickler.deserialize(buffer1);
    assertThat(deserialized1).isEqualTo(record1);

    final var record2 = new InterfaceTypesRecord(new Cat("Whiskers"));
    final var maxSizeOfRecord2 = pickler.maxSizeOf(record2);
    final ByteBuffer buffer2 = ByteBuffer.allocate(maxSizeOfRecord2 + 64);
    final int bytesWritten2 = pickler.serialize(buffer2, record2);
    assertThat(bytesWritten2).isLessThanOrEqualTo(maxSizeOfRecord2);
    buffer2.flip();
    final var deserialized2 = pickler.deserialize(buffer2);
    assertThat(deserialized2).isEqualTo(record2);
  }

  @Test
  void testRecursiveTypes() {
    final var tree = new TreeNode("root", new TreeNode("left", null, null), new TreeNode("right", null, null));
    final var record = new RecursiveTypesRecord(tree);
    final var pickler = Pickler.forClass(RecursiveTypesRecord.class);
    final var maxSizeOfRecord = pickler.maxSizeOf(record);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
    final int bytesWritten = pickler.serialize(buffer, record);
    assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    // TreeNode has circular dependency - should use standard ManySerde
    assertThat(pickler).isInstanceOf(ManySerde.class);
    assertThat(deserialized).isEqualTo(record);
  }

  // Linear dependency test types (no cycles)
  public record Address(String street, String city, String zip) {}
  public record Customer(String name, String email, Address address) {}
  public record Product(String sku, String name, double price) {}
  public record OrderItem(Product product, int quantity) {}
  public record Order(Customer customer, OrderItem[] items, String orderDate) {}

  @Test
  void testLinearDependencyOptimization() {
    // This hierarchy has NO circular dependencies - should be optimized
    final var address = new Address("123 Main St", "Springfield", "12345");
    final var customer = new Customer("John Doe", "john@example.com", address);
    final var product = new Product("SKU001", "Widget", 19.99);
    final var orderItem = new OrderItem(product, 2);
    final var order = new Order(customer, new OrderItem[]{orderItem}, "2024-01-15");
    
    final var pickler = Pickler.forClass(Order.class);
    
    // Should use optimized ManySerde with immutable maps (Map.copyOf)
    assertThat(pickler).isInstanceOf(ManySerde.class);
    final var manySerde = (ManySerde<?>) pickler;
    
    // Test serialization/deserialization works correctly
    final var maxSizeOfRecord = pickler.maxSizeOf(order);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64);
    final int bytesWritten = pickler.serialize(buffer, order);
    assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized.customer()).isEqualTo(order.customer());
    assertThat(deserialized.orderDate()).isEqualTo(order.orderDate());
    assertArrayEquals(deserialized.items(), order.items());
  }

  // Circular dependency test types
  public record NodeA(String name, NodeB nodeB) {}
  public record NodeB(String name, NodeA nodeA) {}

  @Test
  void testCircularDependencyFallback() {
    // This hierarchy has circular dependencies - should use standard resolution
    final var nodeA = new NodeA("A", null);
    final var nodeB = new NodeB("B", nodeA);
    final var updatedNodeA = new NodeA("A", nodeB);
    
    final var pickler = Pickler.forClass(NodeA.class);
    
    // Should use standard ManySerde with lazy resolution
    assertThat(pickler).isInstanceOf(ManySerde.class);
    
    // Test basic functionality (even though we can't serialize circular refs)
    assertThat(pickler.maxSizeOf(updatedNodeA)).isGreaterThan(0);
  }
}

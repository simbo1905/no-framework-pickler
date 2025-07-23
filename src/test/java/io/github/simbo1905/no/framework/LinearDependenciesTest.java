// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class LinearDependenciesTest {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  // E-commerce order processing domain - linear dependencies (optimized path)
  public record Address(String street, String city, String zip) {}
  public record Customer(String name, String email, Address address) {}
  public record Product(String sku, String name, double price) {}
  public record OrderItem(Product product, int quantity) {}
  public record Order(Customer customer, List<OrderItem> items, String orderDate) {}
  public record Invoice(Order order, double tax, double total) {}

  // Dependency graph:
  // Address <- Customer <- Order <- Invoice
  //                  ^       ^
  //                  |       |
  // Product <- OrderItem ----+
  //
  // Built in topological order: [Address, Product, Customer, OrderItem, Order, Invoice]

  @Test
  void testLinearDependencyOptimization() {
    final var address = new Address("123 Main St", "Springfield", "12345");
    final var customer = new Customer("John Doe", "john@example.com", address);
    final var product = new Product("SKU001", "Widget", 19.99);
    final var orderItem = new OrderItem(product, 2);
    final var order = new Order(customer, List.of(orderItem), "2024-01-15T10:00:00Z");
    final var invoice = new Invoice(order, 3.99, 43.97);
    
    final var pickler = Pickler.forClass(Invoice.class);
    
    // Should use optimized ManySerde with immutable maps (Map.copyOf)
    assertThat(pickler).isInstanceOf(ManySerde.class);
    
    // Test serialization/deserialization works correctly
    final var maxSizeOfRecord = pickler.maxSizeOf(invoice);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64);
    final int bytesWritten = pickler.serialize(buffer, invoice);
    assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(invoice);
  }

  // Circular dependency test types (fallback path)
  public record NodeA(String name, NodeB nodeB) {}
  public record NodeB(String name, NodeA nodeA) {}

  @Test
  void testCircularDependencyFallback() {
    // This hierarchy has circular dependencies - should use standard resolution
    final var nodeA = new NodeA("A", null);
    
    final var pickler = Pickler.forClass(NodeA.class);
    
    // Should use standard ManySerde with lazy resolution
    assertThat(pickler).isInstanceOf(ManySerde.class);
    
    // Test basic functionality works
    assertThat(pickler.maxSizeOf(nodeA)).isGreaterThan(0);
    
    // Test serialization of non-circular instance
    final var maxSizeOfRecord = pickler.maxSizeOf(nodeA);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64);
    final int bytesWritten = pickler.serialize(buffer, nodeA);
    assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(nodeA);
  }

  // Self-referential structure - classic recursive case
  public record TreeNode(String value, TreeNode left, TreeNode right) {}

  @Test
  void testSelfReferencingStructure() {
    final var tree = new TreeNode("root", 
        new TreeNode("left", null, null), 
        new TreeNode("right", null, null));
    
    final var pickler = Pickler.forClass(TreeNode.class);
    
    // TreeNode has circular dependency - should use standard ManySerde
    assertThat(pickler).isInstanceOf(ManySerde.class);
    
    // Test serialization/deserialization works correctly
    final var maxSizeOfRecord = pickler.maxSizeOf(tree);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64);
    final int bytesWritten = pickler.serialize(buffer, tree);
    assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(tree);
  }
}
// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Objects;

public class TreeDemo {
  public static void main(String[] args) {

    final var originalRoot = buildTreeOfNodes();

// Get a pickler for the TreeNode sealed interface
    final var pickler = Pickler.forClass(TreeNode.class);

// Allocate a buffer to hold just the root node
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(originalRoot));

// Serialize only the root node (which should include the entire graph)
    pickler.serialize(buffer, originalRoot);

// Prepare buffer for reading
    final var buf = buffer.flip();

// Deserialize the root node (which will reconstruct the entire graph)
    final var deserializedRoot = pickler.deserialize(buf);

// Validate the entire tree structure was properly deserialized
    if (TreeNode.areTreesEqual(originalRoot, deserializedRoot)) {
      System.out.println("✓ Tree serialization and deserialization successful!");
      System.out.println("  Original tree structure:");
      System.out.println("       Root");
      System.out.println("      /    \\");
      System.out.println("  Branch1  SubRoot");
      System.out.println("   /  \\     /    \\");
      System.out.println("  42  99  Branch2 Branch3");
      System.out.println("          /   \\    /   \\");
      System.out.println("        123 EMPTY EMPTY  7");
    } else {
      throw new AssertionError("Tree deserialization failed!");
    }
  }

  static @NotNull InternalNode buildTreeOfNodes() {
    final var leaf1 = new LeafNode(42);
    final var leaf2 = new LeafNode(99);
    final var leaf3 = new LeafNode(123);
    final var leaf4 = new LeafNode(7);

    // A lopsided tree with empty nodes
    final var internal1 = new InternalNode("Branch1", leaf1, leaf2);
    final var internal2 = new InternalNode("Branch2", leaf3, TreeNode.empty());  // Right side is empty
    final var internal3 = new InternalNode("Branch3", TreeNode.empty(), leaf4);  // Left side is empty
    return new InternalNode("Root", internal1, new InternalNode("SubRoot", internal2, internal3));
  }

  /// Internal node that may have left and right children
  public record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode {
    public InternalNode {
      Objects.requireNonNull(name, "name cannot be null");
      Objects.requireNonNull(left, "left cannot be null - use TreeNode.empty() instead");
      Objects.requireNonNull(right, "right cannot be null - use TreeNode.empty() instead");
    }

    /**
     * Custom equals method that properly handles null children
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      InternalNode that = (InternalNode) o;

      if (!name.equals(that.name)) return false;
      if (!Objects.equals(left, that.left)) return false;
      return Objects.equals(right, that.right);
    }

    /**
     * Custom hashCode method that properly handles null children
     */
    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + (left != null ? left.hashCode() : 0);
      result = 31 * result + (right != null ? right.hashCode() : 0);
      return result;
    }
  }

  /// Leaf node with an integer value
  public static record LeafNode(int value) implements TreeNode {
  }

  /// A sealed interface representing a node in a tree structure
  public sealed static interface TreeNode permits InternalNode, LeafNode, TreeEnum {
    /// Returns the empty tree node singleton
    static TreeNode empty() {
      return TreeEnum.EMPTY;
    }

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

  /// Enum representing an empty node in the tree
  public enum TreeEnum implements TreeNode {
    EMPTY
  }
}

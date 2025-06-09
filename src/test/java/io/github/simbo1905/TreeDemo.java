package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;
import io.github.simbo1905.no.framework.tree.InternalNode;
import io.github.simbo1905.no.framework.tree.LeafNode;
import io.github.simbo1905.no.framework.tree.TreeNode;

import java.nio.ByteBuffer;

public class TreeDemo {
  public static void main(String[] args) {

    final var leaf1 = new LeafNode(42);
    final var leaf2 = new LeafNode(99);
    final var leaf3 = new LeafNode(123);
    final var leaf4 = new LeafNode(7);
    
    // A lopsided tree with empty nodes
    final var internal1 = new InternalNode("Branch1", leaf1, leaf2);
    final var internal2 = new InternalNode("Branch2", leaf3, TreeNode.empty());  // Right side is empty
    final var internal3 = new InternalNode("Branch3", TreeNode.empty(), leaf4);  // Left side is empty
    final var originalRoot = new InternalNode("Root", internal1, new InternalNode("SubRoot", internal2, internal3));

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
}

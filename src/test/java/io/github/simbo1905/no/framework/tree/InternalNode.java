// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
package io.github.simbo1905.no.framework.tree;

import java.util.Objects;

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

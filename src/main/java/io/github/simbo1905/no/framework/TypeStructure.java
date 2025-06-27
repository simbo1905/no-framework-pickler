// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;


import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Tag.*;

record TagWithType(Tag tag, Class<?> type) {
}

/// TypeStructure record for analyzing generic types
record TypeStructure(List<TagWithType> tagTypes) {

  static final int MAP_TYPE_ARG_COUNT = 2;

  TypeStructure(List<Tag> tags, List<Class<?>> types) {
    this(IntStream.range(0, Math.min(tags.size(), types.size()))
        .mapToObj(i -> new TagWithType(tags.get(i), types.get(i)))
        .toList());
    if (tags.size() != types.size()) {
      throw new IllegalArgumentException("Tags and types lists must have same size: tags=" + tags.size() + " types=" + types.size() +
          " tags=" + tags + " types=" + types);
    }
  }

  /// Analyze a generic type and extract its structure
  ///
  /// This method is the entry point for the recursive metaprogramming pattern. It performs
  /// static analysis on a Java Type to decompose it into a chain of container types
  /// followed by a leaf type.
  ///
  /// The analysis recursively unwraps:
  /// - Generic types (List<T>, Map<K,V>, Optional<T>)
  /// - Array types (T[], T[][], etc.)
  /// - Raw types to their component types
  ///
  /// The result is a TypeStructure containing a list like:
  /// [LIST, ARRAY, MAP, RECORD] for a type like List<String[][]>
  ///
  /// This static analysis enables the builder methods (buildWriterChain, buildReaderChain,
  /// buildSizerChain) to construct appropriate delegation chains at construction time,
  /// avoiding any runtime type inspection during serialization/deserialization.
  ///
  /// The pattern supports arbitrary nesting depth - e.g., List<Map<String, Optional<Integer[]>[]>>
  /// is fully supported without any special-case code.
  static TypeStructure analyze(Type type) {
    List<Tag> tags = new ArrayList<>();
    List<Class<?>> types = new ArrayList<>();

    Object current = type;

    while (current != null) {
      switch (current) {
        case ParameterizedType paramType -> {
          Type rawType = paramType.getRawType();

          if (rawType.equals(List.class)) {
            tags.add(LIST);
            types.add(List.class); // Container class for symmetry with Arrays.class pattern
            Type[] typeArgs = paramType.getActualTypeArguments();
            current = typeArgs.length > 0 ? typeArgs[0] : null;
          } else if (rawType.equals(Map.class)) {
            tags.add(MAP);
            types.add(Map.class); // Container class for symmetry
            Type[] typeArgs = paramType.getActualTypeArguments();
            if (typeArgs.length == MAP_TYPE_ARG_COUNT) {
              // For Map<K,V>, we need to analyze both key and value types
              // Store Map marker, then key structure, then value structure
              TypeStructure keyStructure = analyze(typeArgs[0]);
              TypeStructure valueStructure = analyze(typeArgs[1]);

              // Combine: [MAP, ...key tags, ...value tags]
              for (TagWithType tt : keyStructure.tagTypes()) {
                tags.add(tt.tag());
                types.add(tt.type());
              }
              for (TagWithType tt : valueStructure.tagTypes()) {
                tags.add(tt.tag());
                types.add(tt.type());
              }
            }
            return new TypeStructure(tags, types);
          } else if (rawType.equals(Optional.class)) {
            tags.add(OPTIONAL);
            types.add(Optional.class); // Container class for symmetry
            Type[] typeArgs = paramType.getActualTypeArguments();
            current = typeArgs.length > 0 ? typeArgs[0] : null;
          } else {
            // Unknown parameterized type, treat as raw type
            current = rawType;
          }
        }
        case GenericArrayType arrayType -> {
          // Handle arrays of parameterized types like Optional<String>[]
          tags.add(ARRAY);
          types.add(Arrays.class); // Arrays.class as marker

          current = arrayType.getGenericComponentType(); // Continue processing element type
        }
        case Class<?> clazz -> {
          if (clazz.isArray()) {
            // Array container with element type, e.g. short[] -> [ARRAY, SHORT]
            tags.add(ARRAY);
            types.add(Arrays.class); // Arrays.class as marker, not concrete array type
            current = clazz.getComponentType(); // Continue processing element type
          } else {
            // Regular class - terminal case
            tags.add(fromClass(clazz));
            types.add(clazz);
            return new TypeStructure(tags, types);
          }
        }
        default -> {
          // Unknown type, return what we have
          return new TypeStructure(tags, types);
        }
      }
    }

    return new TypeStructure(tags, types);
  }
}

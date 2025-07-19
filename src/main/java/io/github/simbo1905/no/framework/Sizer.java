// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.util.function.ToIntFunction;

interface Sizer extends ToIntFunction<Object> {
  /// Get the size of an object of the given class
  default int sizeOf(Object obj) {
    return applyAsInt(obj);
  }

  interface Resolver {
  }
}

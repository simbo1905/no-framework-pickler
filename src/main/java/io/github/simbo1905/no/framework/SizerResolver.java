// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.util.function.Function;

interface SizerResolver extends
    Function<Class<?>, Sizer> {
  // For this simple case, the resolvers can be simple lambdas that throw
  // as they shouldn't be called for a trivial record with only primitive components.
  SizerResolver throwsSizerResolver = type -> {
    throw new AssertionError("Sizer throwsSizerResolver should not be reachable.");
  };
}

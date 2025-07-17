// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.util.function.Function;

interface ReaderResolver extends
    Function<Long, Reader> {
  ReaderResolver throwsReaderResolver = type -> {
    throw new AssertionError("Reader throwsReaderResolver should not be reachable.");
  };
}

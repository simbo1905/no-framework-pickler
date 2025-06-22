// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;

/// NilPickler is a marker pickler that is a placeholder rather than having a null in an array of picklers.
/// This allows us to have a pickler for every user record type but no pickler for enum or sealed interface types.
/// It would be a bug to try to serialize or deserialize using this pickler, as it is not intended for any data.
@SuppressWarnings("rawtypes")
enum NilPickler implements Pickler {
  INSTANCE;

  @Override
  public int serialize(ByteBuffer buffer, Object record) {
    throw new UnsupportedOperationException("NilPickler cannot serialize any data");
  }

  @Override
  public Object deserialize(ByteBuffer buffer) {
    throw new UnsupportedOperationException("NilPickler cannot deserialize any data");
  }

  @Override
  public int maxSizeOf(Object record) {
    throw new UnsupportedOperationException("NilPickler cannot size any data");
  }
}

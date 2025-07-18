// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

/// Main coordinator for multiple record types using static analysis and callback delegation
final class PicklerImpl<R> implements Pickler<R> {
  final Class<R> rootClass;
  final Map<Class<?>, Pickler<?>> serdes;
  final Map<Long, Pickler<?>> typeSignatureToSerde;

  PicklerImpl(Class<R> rootClass,
              Map<Class<?>, Pickler<?>> serdes,
              Map<Long, Pickler<?>> typeSignatureToSerde) {
    this.rootClass = rootClass;
    this.serdes = Map.copyOf(serdes);
    this.typeSignatureToSerde = Map.copyOf(typeSignatureToSerde);
  }


  @Override
  public int serialize(ByteBuffer buffer, R record) {
    Objects.requireNonNull(buffer);
    Objects.requireNonNull(record);
    @SuppressWarnings("unchecked") final var serde = (Pickler<R>) serdes.get(record.getClass());
    if (serde == null) {
      throw new IllegalArgumentException("No serde found for class: " + record.getClass());
    }
    return serde.serialize(buffer, record);
  }

  @Override
  public R deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    final long typeSignature = buffer.getLong();
    final var serde = typeSignatureToSerde.get(typeSignature);
    if (serde == null) {
      throw new IllegalStateException("Unknown type signature: " + typeSignature);
    }
    final R result;
    if (serde instanceof RecordSerde<?> recordSerde) {
      //noinspection unchecked
      result = (R) recordSerde.deserializeWithoutSignature(buffer);
    } else if (serde instanceof EmptyRecordSerde<?> emptyRecordSerde) {
      //noinspection unchecked
      result = (R) emptyRecordSerde.deserializeWithoutSignature(buffer);
    } else if (serde instanceof EnumPickler<?> enumPickler) {
      //noinspection unchecked
      result = (R) enumPickler.deserializeWithoutSignature(buffer);
    } else {
      throw new IllegalStateException("Unsupported serde type: " + serde.getClass());
    }
    return result;
  }

  @Override
  public int maxSizeOf(R record) {
    Objects.requireNonNull(record);
    @SuppressWarnings("unchecked") final var serde = (Pickler<R>) serdes.get(record.getClass());
    if (serde == null) {
      throw new IllegalArgumentException("No serde found for class: " + record.getClass());
    }
    return serde.maxSizeOf(record);
  }

  @Override
  public long typeSignature(Class<?> originalClass) {
    final var serde = serdes.get(originalClass);
    if (serde == null) {
      throw new IllegalArgumentException("No serde found for class: " + originalClass);
    }
    return serde.typeSignature(originalClass);
  }
}

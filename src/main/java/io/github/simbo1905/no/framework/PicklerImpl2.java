// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.github.simbo1905.no.framework.Companion.recordClassHierarchy;

/// Main coordinator for multiple record types using static analysis and callback delegation
final class PicklerImpl2<R> implements Pickler2<R> {
  final Class<R> rootClass;
  final Map<Class<?>, Long> typeSignatures;
  final Map<Long, Pickler2<?>> typeSignatureToPicklerMap;
  final Map<Class<?>, Pickler2<?>> classToPicklerMap;

  PicklerImpl2(Class<R> rootClass, Map<Class<?>, Long> altTypeSignatures) {
    this.rootClass = rootClass;
    final var classHierarchy = recordClassHierarchy(rootClass);

    final var recordClasses = classHierarchy.stream().filter(Class::isRecord).collect(Collectors.toList());
    this.typeSignatures = Companion.computeRecordTypeSignatures(recordClasses);

    this.classToPicklerMap = recordClasses.stream()
        .collect(Collectors.toMap(
            Function.identity(),
            cls -> {
              final var typeSignature = typeSignatures.get(cls);
              final var altTypeSignature = Optional.ofNullable(altTypeSignatures.get(cls));
              if (cls.getRecordComponents().length == 0) {
                return new EmptyRecordSerde2<>(cls, typeSignature, altTypeSignature);
              }
              return new RecordSerde2<>(
                  cls,
                  typeSignature,
                  altTypeSignature,
                  this::resolveTypeSizer,
                  this::resolveTypeWriter,
                  this::resolveTypeReader
              );
            }
        ));

    this.typeSignatureToPicklerMap = classToPicklerMap.entrySet().stream()
        .collect(Collectors.toMap(entry -> typeSignatures.get(entry.getKey()), Map.Entry::getValue));
  }

  @SuppressWarnings("unchecked")
  <X> Sizer resolveTypeSizer(Class<X> targetClass) {
    return obj -> {
      if (obj == null) return Byte.BYTES;
      final Pickler2<X> pickler = (Pickler2<X>) witness(targetClass);
      if (pickler != null) {
        return pickler.maxSizeOf((X) obj);
      }
      throw new UnsupportedOperationException("Unhandled type: " + targetClass);
    };
  }

  private <X> Pickler2<?> witness(Class<X> targetClass) {
    return classToPicklerMap.get(targetClass);
  }

  @SuppressWarnings("unchecked")
  <X> Writer resolveTypeWriter(Class<X> targetClass) {
    return (buffer, obj) -> {
      final Pickler2<X> pickler = (Pickler2<X>) classToPicklerMap.get(targetClass);
      if (pickler != null) {
        final var positionBefore = buffer.position();
        final int bytesWritten = pickler.serialize(buffer, (X) obj);
        LOGGER.fine(() -> "Serialized " + targetClass.getSimpleName() + " to " + bytesWritten + " bytes at position " + positionBefore);
        return;
      }
      throw new UnsupportedOperationException("Unhandled type: " + targetClass);
    };
  }

  Reader resolveTypeReader(Long typeSignature) {
    LOGGER.fine(() -> "resolveTypeReader called with typeSignature: 0x" + Long.toHexString(typeSignature));
    return buffer -> {
      if (typeSignature == 0L) {
        LOGGER.fine(() -> "Returning null for typeSignature 0L");
        return null;
      }

      LOGGER.finer(() -> "Looking up typeSignature 0x" + Long.toHexString(typeSignature) + " in map");
      final Pickler2<?> pickler = typeSignatureToPicklerMap.get(typeSignature);

      if (pickler == null) {
        LOGGER.warning(() -> "No pickler found for typeSignature 0x" + Long.toHexString(typeSignature));
        throw new UnsupportedOperationException("Unhandled type signature: " + typeSignature);
      }

      LOGGER.fine(() -> "Found pickler " + pickler.getClass().getSimpleName() + " for typeSignature 0x" +
          Long.toHexString(typeSignature));

      // If it's a RecordSerde2, use deserializeWithoutSignature since the signature was already read
      if (pickler instanceof RecordSerde2<?> recordSerde) {
        LOGGER.fine(() -> "[PicklerImpl2] Using RecordSerde2.deserializeWithoutSignature for signature 0x" +
            Long.toHexString(typeSignature));
        return recordSerde.deserializeWithoutSignature(buffer);
      }

      // If it's an EmptyRecordSerde2, use deserializeWithoutSignature since the signature was already read
      if (pickler instanceof EmptyRecordSerde2<?> emptyRecordSerde) {
        LOGGER.fine(() -> "[PicklerImpl2] Using EmptyRecordSerde2.deserializeWithoutSignature for signature 0x" +
            Long.toHexString(typeSignature));
        return emptyRecordSerde.deserializeWithoutSignature(buffer);
      }

      LOGGER.finer(() -> "Starting deserialization of typeSignature 0x" + Long.toHexString(typeSignature) +
          " at buffer position " + buffer.position() + " with " + buffer.remaining() + " bytes remaining");
      return pickler.deserialize(buffer);
    };
  }

  @Override
  public int serialize(ByteBuffer buffer, R record) {
    Objects.requireNonNull(buffer);
    Objects.requireNonNull(record);
    @SuppressWarnings("unchecked") final Pickler2<R> pickler = (Pickler2<R>) witness(record.getClass());
    return pickler.serialize(buffer, record);
  }

  @Override
  public R deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    final long typeSignature = buffer.getLong();
    buffer.position(buffer.position() - Long.BYTES); // Rewind
    final Pickler2<?> pickler = typeSignatureToPicklerMap.get(typeSignature);
    if (pickler != null) {
      @SuppressWarnings("unchecked") final R result = (R) pickler.deserialize(buffer);
      return result;
    }
    throw new UnsupportedOperationException("Unhandled type signature: " + typeSignature);
  }

  @Override
  public int maxSizeOf(R record) {
    Objects.requireNonNull(record);
    @SuppressWarnings("unchecked") final Pickler2<R> pickler = (Pickler2<R>) classToPicklerMap.get(record.getClass());
    return pickler.maxSizeOf(record);
  }

  @Override
  public long typeSignature(Class<?> originalClass) {
    return typeSignatures.get(originalClass);
  }
}

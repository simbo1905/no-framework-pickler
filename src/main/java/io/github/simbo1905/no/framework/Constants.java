// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

/// Enum containing constants used throughout the Pickler implementation
enum Constants {
  NULL(0),
  BOOLEAN(1),
  BYTE(Byte.BYTES),
  SHORT(Short.BYTES),
  CHARACTER(Character.BYTES),
  INTEGER(Integer.BYTES),
  // FIXME spending a bit to save a byte seems not worth it. Make it configurable to turn on?
  INTEGER_VAR(Integer.BYTES),
  LONG(Long.BYTES),
  // FIXME time in ms saves three bytes but we spend a byte and do work. Make it configurable to turn on?
  LONG_VAR(Long.BYTES),
  FLOAT(Float.BYTES),
  DOUBLE(Double.BYTES),
  STRING(0),
  OPTIONAL_EMPTY(0),
  OPTIONAL_OF(0),
  ENUM(0),
  ARRAY(0),
  ARRAY_OBJ(0),
  MAP(0),
  LIST(0),
  RECORD(0),
  UUID(16);

  final int sizeInBytes;

  Constants(int sizeInBytes) {
    this.sizeInBytes = sizeInBytes;
  }

  int marker() {
    // Use the Constants enum ordinal, not the tag ordinal!
    return -1 - this.ordinal();
  }
}

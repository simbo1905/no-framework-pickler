// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

/// Enum containing constants used throughout the Pickler implementation
enum Constants {
  NULL(0, null),
  BOOLEAN(1, Tag.BOOLEAN),
  BYTE(Byte.BYTES, Tag.BYTE),
  SHORT(Short.BYTES, Tag.SHORT),
  CHARACTER(Character.BYTES, Tag.CHARACTER),
  INTEGER(Integer.BYTES, Tag.INTEGER),
  // FIXME spending a bit to save a byte seems not worth it. Make it configurable to turn on?
  INTEGER_VAR(Integer.BYTES, Tag.INTEGER),
  LONG(Long.BYTES, Tag.LONG),
  // FIXME time in ms saves three bytes but we spend a byte and do work. Make it configurable to turn on?
  LONG_VAR(Long.BYTES, Tag.LONG),
  FLOAT(Float.BYTES, Tag.FLOAT),
  DOUBLE(Double.BYTES, Tag.DOUBLE),
  STRING(0, Tag.STRING),
  OPTIONAL_EMPTY(0, Tag.OPTIONAL),
  OPTIONAL_OF(0, Tag.OPTIONAL),
  ENUM(0, Tag.ENUM),
  ARRAY(0, Tag.ARRAY),
  MAP(0, Tag.MAP),
  LIST(0, Tag.LIST),
  RECORD(0, Tag.RECORD),
  UUID(16, Tag.UUID);

  final int sizeInBytes;
  final Tag tag;

  Constants(int sizeInBytes, Tag tag) {
    this.sizeInBytes = sizeInBytes;
    this.tag = tag;
  }

  int marker() {
    // Use the Constants enum ordinal, not the tag ordinal!
    return -1 - this.ordinal();
  }
}

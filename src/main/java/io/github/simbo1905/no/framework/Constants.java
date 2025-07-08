// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

/// Enum containing constants used throughout the Pickler implementation
enum Constants {
  // as memory in java is zeroed we do not use 0 on the wire.
  UNUSED,
  BOOLEAN,
  BYTE,
  SHORT,
  CHARACTER,
  INTEGER,
  // FIXME spending a bit to save a byte seems not worth it. Make it configurable to turn on?
  INTEGER_VAR,
  LONG,
  // FIXME time in ms saves three bytes but we spend a byte and do work. Make it configurable to turn on?
  LONG_VAR,
  FLOAT,
  DOUBLE,
  STRING,
  OPTIONAL_EMPTY,
  OPTIONAL_OF,
  ARRAY,
  MAP,
  LIST,
  ARRAY_RECORD,
  ARRAY_INTERFACE,
  ARRAY_ENUM,
  ARRAY_BOOLEAN,
  ARRAY_BYTE,
  ARRAY_SHORT,
  ARRAY_CHAR,
  ARRAY_INT,
  ARRAY_LONG,
  ARRAY_FLOAT,
  ARRAY_DOUBLE,
  ARRAY_STRING,
  ARRAY_UUID,
  ARRAY_LOCAL_DATE,
  ARRAY_LOCAL_DATE_TIME,
  ARRAY_ARRAY, ARRAY_LIST, ARRAY_MAP, ARRAY_OPTIONAL;

  int marker() {
    // Use the Constants enum ordinal, not the tag ordinal!
    return -1 - this.ordinal();
  }
}

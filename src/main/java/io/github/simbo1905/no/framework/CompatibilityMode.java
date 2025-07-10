package io.github.simbo1905.no.framework;

import java.util.Arrays;

/// Compatibility mode. Set via system property `no.framework.Pickler.Compatibility`. The default is DISABLED.
/// **Record Types** If set to DISABLED (our default) the pickler will during deserialization of `records`:
/// - verify 8-bytes long taken the first 8 bytes of sha256 packed into a long hash of class simple nane as well as components full generic types and name.
/// If set to DEFAULTED (the opt-in), the pickler will:
/// - accept an optional
/// - defaults missing component fields to null for reference types and default values for primitives (e.g., 0 for int).
/// - bypass the hash check
/// This allows for renaming of fields but not reordering.
///
/// **Enum Types** If set to DISABLED (our default) the pickler will during deserialization of `enums`:
/// - verify 8-bytes of a sha256 hash of class simple name as well as enum constant names
/// If set to DEFAULTED (the opt-in), the pickler will:
/// - bypass the hash check
///
/// In contrast, JDK deserialization using ObjectInputStream:
///  - defaults missing component fields in the wire. Reference types are null, primitives use default values (e.g., 0 for int).
///  - writes out the class name, component names, and types onto the wire.
/// This allows for reordering of fields but not renaming.
enum CompatibilityMode {
  /// Strict mode: Disallows any schema changes. Verifies hashes, types, and length.
  /// Throws exceptions for any mismatches or unexpected fields.
  DISABLED,

  /// Lenient mode: Allows missing fields (uses defaults) but does not permit field reordering.
  /// More permissive than ENABLED but differs from JDK behavior by disallowing reordering.
  ENABLED;

  static CompatibilityMode current() {
    final String mode = System.getProperty("no.framework.Pickler.Compatibility", "DISABLED").toUpperCase();
    try {
      return CompatibilityMode.valueOf(mode);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid compatibility mode: " + mode + ". Must be one of: " + Arrays.toString(CompatibilityMode.values()));
    }
  }
}

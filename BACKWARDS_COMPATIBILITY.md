


When `DISABLED` which is our default model to make the feature opt-in:
- We compute a hash of the type signature at pickler creation time:
    - `records` uses 8-byes of a SHA256 of the record class simple name, component types and names
    - `enums` uses 8-byes of a SHA256 of the enum class simple name and enum constant names
- The precomputed hash written out as a `long` value
- The written `long` is read back at deserialization time and compared against the local precomputed version.
- The runtime check is `long != long` which is a fast primitive operation.

When `ENABLED` mode is enabled:
- **Only Append** new component fields to the end of `record` definitions (this avoids all the issues detailed below)
- **Only Append** new enum constants to the end of `enum` definitions (this avoids all the issues detailed below)
- **Never change field types** as this will cause deserialization errors (just like JDK serialization)
- **Component renaming works** for records (unlike JDK serialization which writes out names)
- **Enum constant renaming works** for records (unlike JDK serialization which writes out names)
- **Never reorder existing fields** as this may cause silent data corruption (unlike JDK serialization which writes out names)

This means you are safe by default and the cost of writing and reading the `long` is far less than the cost of writing out all the field names or enum constant names to the wire.

When enabled the `ENABLED` mode emulates the JDK serialization behavior with the following differences:
- `java.io.ObjectInputStream`:
    - Fails fast if you have renamed `enum` constants or `record` components.
    - Has no problem with reordering `enum` constants or `record`.
- No Framework Pickler:
    - Fails fast if you have reordered `record` components that have different raw or generic types.
    - Has no problem with renaming `enum` constants or `record` components
    - as long as you do not reorder them.
- No Framework Pickler fails fast only if the reordered values have different raw or generic types.
- No Framework Pickler does not detect reordered values that have the same raw or generic types.


#### For Record Types:
- **`DISABLED`** (default): Strict mode. Verifies 8-byte SHA256 hash of class simple name, component types and names. Throws exceptions for any mismatches.
- **`ENABLED`**: Backwards compatibility mode. Bypasses hash check and allows missing fields:
    - Missing fields get default values: primitives get `0`, `false`, `0.0`, etc.
    - Reference types (including arrays) get `null`
    - Allows safe field renaming but NOT reordering

#### For Enum Types:
- **`DISABLED`** (default): Strict mode. Verifies 8-byte SHA256 hash of class simple name and enum constant names.
- **`ENABLED`**: Backwards compatibility mode. Bypasses hash check.
    - Enums are serialized as ordinals (not names) for better compression
    - Allows safe enum constant renaming but NOT reordering

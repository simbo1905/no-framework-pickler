# Backwards Compatibility in Framework Pickler

## Project Overview

When `DISABLED` which is our default model to make the feature opt-in:

- We compute a hash of the type signature at pickler creation time:
    - `records` uses the first 8-byes of a SHA256 of the record class full name, component types and names
    - `enums` uses the first 8-byes of a SHA256 of the enum class full name and enum constant names
- The precomputed hash written out as a `long` value
- The written `long` is read back at deserialization time and compared against the local precomputed version.
- The runtime check is `long != long` which is a fast primitive operation.
- This will throw an exception if there is not a match.

This means you are safe by default and the cost of writing and reading the `long` is far less than the cost of writing
out all the field names or enum constant names to the wire.

When `ENABLED` mode is enabled:

- You must supply and additional `Map<Class<?>,Long>` that supplies the alternative type signatures written by the older
  codebase.
- You look up what are the older original type signatures using the method `pickler.typeSignature(MyRecord.class)`.
- You must then construct the new pickler passing in a map of the alternative type signatures passing the map to the
  pickler factory method e.g.
  `var p = Pickler.forClass(Animals.class, Map.of( Penguin.class, long1, Alicorn.class, long2 ));`.
- **Only Append** new component fields to the end of `record` definitions (this avoids all the issues detailed below)
- **Only Append** new enum constants to the end of `enum` definitions (this avoids all the issues detailed below)
- **Never change record component types** as this will cause deserialization errors (just like JDK serialization)
- **Component renaming works** for records (unlike JDK serialization which writes out names)
- **Enum constant renaming works** for records (unlike JDK serialization which writes out names)
- **Never reorder existing fields** as this may cause silent data corruption (unlike JDK serialization which writes out
  names)

When enabled the `ENABLED` mode is inspired by the JDK serialization behavior that it will use the default values for
record components yet as we do not write out the field names this can only work for **appending** new fields to the end
of the record definition.

When compared to JDK Serialization there are the following differences:

- `java.io.ObjectInputStream`:
    - Fails fast if you have renamed `enum` constants or `record` components.
    - Has no problem with reordering `enum` constants or `record`.
- No Framework Pickler:
    - Fails fast if you have reordered `record` components that have different raw or generic types.
    - Has no problem with renaming `enum` constants or `record` components as long as you do not reorder them.
- No Framework Pickler fails fast only if the reordered values have different raw or generic types.
- No Framework Pickler does not detect reordered values that have the same raw or generic types.

## Compatibility Mode

The compatibility mode is handled in `CompatibilityMode.java`. The compatibility mode can be set via system property
`no.framework.Pickler.Compatibility`. The default is `DISABLED`.

- **DISABLED**: The safe mode which is the default mode. Strictly no backwards compatibility. Fails fast on any schema
  mismatch.
- **ENABLED**: Opt-in mode. Emulates JDK serialization functionality with a risky corner case.
- If you do opt-in **never** reorder existing fields or enum constants in your source file to avoid the risky corner
  case of "swapping" components or enum constants when deserializing.

End.

# No Framework Pickler Architecture

## Project Overview

**No Framework Pickler** is a lightweight, zero-dependency Java 21+ serialization library that generates type-safe,
compact and fast, serializers for records containing value-like types as well as container-like types. This includes
value-like types such as `UUID`, `String`, `enum` was well as container-like such as optionals, arrays, lists, and maps
of value-like types.

The library avoids deep reflection on the Object-stage (Runtime) "hot path"  for performance.
All reflective operations are done during Meta-stage (Construction Time) in `Pickler.forClass(Class<?>)` on the public
API of records.
This construction resolves public method handles and "unreflects" them to get direct method handles. It then creates
functions that are delegation chains that are used at runtime without further reflection. This is done by constructing
type specific serializers using:

- **Abstract Syntax Tree construction** for the parameterized types of `record` components
- **Multi-stage programming** with a meta-stage during `Pickler.forClass(Class<?>)` construction that performs **static
  semantic analysis** to build an **Abstract Syntax Tree (AST)** representation of the type structure
- **Static semantic analysis** preserving type safety guarantees

The implementation uses a **parallel sequence of structural tags and concrete types** to represent the type structure.

## Recursive Type Analysis Algorithm

The analysis performs **recursive descent parsing** of Java's `Type` hierarchy:

1. **Container Recognition**: Identifies parameterized types (List<T>, Map<K,V>, Optional<T>) and arrays (T[])
2. **Recursive Decomposition**: Recursively analyzes type arguments to arbitrary depth
3. **AST Construction**: Builds parallel sequences of structural tags and concrete types
4. **Termination**: Reaches leaf nodes at primitive/user-defined types

## Abstract Syntax Tree (AST) Construction and Formal Grammar

The **static semantic analysis** implements **recursive descent parsing** of Java's Type hierarchy to construct *
*Abstract Syntax Trees** (AST) that represent the nested container structure of parameterized types. This AST
construction enables **compile-time specialization** through **multi-stage programming**.

The AST distinguishes between value and container types at the implementation level:

- **ValueNode**: Represents logical values rather than containers (array, list, map, optional, ...)
    - **PrimitiveValueNode**: Represents Java primitive types (`int.class`, `boolean.class`, etc.)
    - **RefValueNode**: Represents reference types including boxed primitives (`Integer.class`, `Boolean.class`),
      `String`, `UUID`, enums, records, and interfaces
- **Containers**: Represents arrays, list, optionals, and maps
    - **ArrayNode**: Represents arrays (e.g., `int[]`, `Integer[]`)
    - **ListNode**: Represents lists (e.g., `List<String>`)
    - **OptionalNode**: Represents optionals (e.g., `Optional<Double>`)
    - **MapNode**: Represents maps (e.g., `Map<String, Integer>`)

At runtime, we also make a distinction between built-in and user types. With user types we support both records, enums
and nested sealed interfaces of those types

# Readable AST Grammar with Parenthesized Notation

## EBNF Grammar

```ebnf
TypeStructure ::= TypeExpression

TypeExpression ::= ValueType | ContainerExpression

ContainerExpression ::= ArrayExpression 
                     | ListExpression 
                     | OptionalExpression 
                     | MapExpression

ArrayExpression ::= 'ARRAY(' TypeExpression ')'

ListExpression ::= 'LIST(' TypeExpression ')'

OptionalExpression ::= 'OPTIONAL(' TypeExpression ')'

MapExpression ::= 'MAP(' TypeExpression ',' TypeExpression ')'

ValueType ::= PrimitiveType | ReferenceType

PrimitiveType ::= 'BOOLEAN' | 'BYTE' | 'SHORT' | 'CHARACTER' 
                | 'INTEGER' | 'LONG' | 'FLOAT' | 'DOUBLE'

ReferenceType ::= BoxedPrimitive | BuiltInValueBased | UserType | CustomValueBased

BoxedPrimitive ::= 'BOOLEAN' | 'BYTE' | 'SHORT' | 'CHARACTER' 
                 | 'INTEGER' | 'LONG' | 'FLOAT' | 'DOUBLE'

BuiltInValueBased ::= 'STRING' | 'LOCAL_DATE' | 'LOCAL_DATE_TIME'

UserType ::= 'ENUM' | 'RECORD' | 'INTERFACE'

CustomValueBased ::= <user-defined value-based types with custom handlers>

Note: PrimitiveType and BoxedPrimitive share names but represent different runtime types (int vs Integer)
```

Note that we capture the actual concrete type into the AST. This means that at runtime we can understand that whether
the INTEGER is a primitive or a reference type. This is important for null-safety and default value handling. When we
run `toTreeString` we output the class getSimpleName() as we will see in the following examples.

## Examples with Tree Structure

### Simple Examples

- `boolean` → `boolean` (primitive)
- `Boolean` → `Boolean` (reference)
- `int[]` → `ARRAY(int)` (primitive array)
- `Integer[]` → `ARRAY(Integer)` (reference array)
- `List<String>` → `LIST(String)`
- `Optional<Double>` → `OPTIONAL(Double)` (reference type)
- `Map<String, Integer>` → `MAP(String, Integer)` (Java maps use reference types and auto-boxing)

### Nested Examples

- `List<Double>[]` → `ARRAY(LIST(Double))`
- `Optional<String[]>` → `OPTIONAL(ARRAY(String))`
- `Map<String, List<Integer>>` → `MAP(String, LIST(Integer))`
- `List<Optional<Boolean>>` → `LIST(OPTIONAL(Boolean))`
- `List<double[]>` → `LIST(ARRAY(double))`

### Complex Nested Example

`List<Map<String, Optional<Integer[]>[]>>`

Breaking it down:

- Outer container: `List<...>`
- Inside List: `Map<String, Optional<Integer[]>[]>`
- Map key: `String`
- Map value: `Optional<Integer[]>[]`
- Inside array: `Optional<Integer[]>`
- Inside Optional: `Integer[]`

**Tree representation:**

```
LIST(
  MAP(
    String,
    ARRAY(
      OPTIONAL(
        ARRAY(Integer)
      )
    )
  )
)
```

## Test Case Generation

With this grammar, you can systematically generate test cases:

### Depth 1 (Primitives only for those that have them)

- `TypeExpr.PrimitiveValueType.BOOLEAN`
- `TypeExpr.PrimitiveValueType.BYTE`
- `TypeExpr.PrimitiveValueType.SHORT`
- `TypeExpr.PrimitiveValueType.CHARACTER`
- `TypeExpr.PrimitiveValueType.INTEGER`
- `TypeExpr.PrimitiveValueType.LONG`
- `TypeExpr.PrimitiveValueType.FLOAT`
- `TypeExpr.PrimitiveValueType.DOUBLE`
- .. repeat for boxed referenced types ..
- `TypeExpr.RefValueType.STRING`
- `TypeExpr.RefValueType.UUID`

### Depth 2 (Single container)

- `ARRAY(P)` for each value type P
- `LIST(P)` for each value type P
- `OPTIONAL(P)` for each value type P
- `MAP(P1, P2)` for each pair of value types P1, P2

### Depth 3 (Nested containers)

- `ARRAY(LIST(P))` → `List<P>[]`
- `LIST(ARRAY(P))` → `List<P[]>`
- `OPTIONAL(MAP(P1, P2))` → `Optional<Map<P1, P2>>`
- `MAP(P, LIST(P))` → `Map<P, List<P>>`
- etc.

## Benefits of This Notation

1. **Clear nesting**: Parentheses show exactly what contains what
2. **Explicit arity**: MAP always has exactly 2 arguments
3. **Easy to parse**: Both humans and machines can easily understand the structure
4. **Direct mapping**: Each expression maps directly to Java generic syntax
5. **Test generation**: Can systematically enumerate all possible type combinations up to a given depth
6. **Primitive vs Reference**: Distinguishes between primitive and reference types, allowing for null-safety
   optimizations.

In the future the JDK with future Valhalla features may generalize forcing a variable to be a value type then the
compiler will not allow you to do certain things with it such as use it as an object monitor. This is proposed as a new
`value` type and also the ability for user types to pass deconstruction pattern. This will mean that a user type may, or
map not, be used as a value type. Then to speed up and inline records onto the stack the users of Java will be able to "
opt into" value types semantics for the user types that by default will have reference types semantics. No Framework
Pickler is humbly attempting to understand how this dualism will work and to be arranged a way that in the future may be
able to take advantage of the future Java features.

#### AST Construction Algorithm

The **recursive descent parser** implements the following algorithm:

1. **Container Recognition**: Identifies parameterized types (List<T>, Map<K,V>, Optional<T>) and arrays (T[])
2. **Recursive Decomposition**: Recursively analyzes type arguments to arbitrary depth using **typing context**
   preservation
3. **AST Construction**: Builds parallel sequences of structural tags and concrete types maintaining **type environment
   ** (Γ)
4. **Termination**: Reaches leaf nodes at value types (primitive, boxed, user enum, user record, value-like UUID,
   String)

## Step-by-Step Parse Tree Construction

### Example Analysis: `List<Map<String, Optional<int[]>[]>>`

```
Input: List<Map<String, Optional<int[]>[]>>

Step 1: Recognize outer container
- Type: List<T> where T = Map<String, Optional<int[]>[]>
- Action: Extract LIST container, recurse on T
- AST so far: LIST(...)

Step 2: Parse T = Map<String, Optional<Integer[]>[]>
- Type: Map<K,V> where K = String, V = Optional<int[]>[]
- Action: Extract MAP container, recurse on K and V
- AST so far: LIST(MAP(..., ...))

Step 3a: Parse K = String
- Type: String (primitive)
- Action: Terminal node, add STRING
- AST so far: LIST(MAP(String, ...))

Step 3b: Parse V = Optional<int[]>[]
- Type: Array of Optional<int[]>
- Action: Extract ARRAY container, recurse on element type
- AST so far: LIST(MAP(String, ARRAY(...)))

Step 4: Parse element = Optional<int[]>
- Type: Optional<T> where T = int[]
- Action: Extract OPTIONAL container, recurse on T
- AST so far: LIST(MAP(String, ARRAY(OPTIONAL(...))))

Step 5: Parse T = int[]
- Type: Array of primative int
- Action: Extract ARRAY container, recurse on element type
- AST so far: LIST(MAP(String, ARRAY(OPTIONAL(ARRAY(...)))))

Step 6: Parse element = int
- Type: int (primitive)
- Action: Terminal node, add int
- AST so far: LIST(MAP(STRING, ARRAY(OPTIONAL(ARRAY(int)))))

Final AST: LIST(MAP(String, ARRAY(OPTIONAL(ARRAY(int)))))
```

### Detailed Trace with Type Environment

```
parseType("List<Map<String, Optional<int[]>[]>>")
├─ recognize: List<...>
├─ extract type parameter: Map<String, Optional<int[]>[]>
└─ return: LIST(parseType("Map<String, Optional<int[]>[]>"))
   │
   └─ parseType("Map<String, Optional<int[]>[]>")
      ├─ recognize: Map<...,...>
      ├─ extract key type: String
      ├─ extract value type: Optional<int[]>[]
      └─ return: MAP(parseType("String"), parseType("Optional<int[]>[]"))
         │
         ├─ parseType("String")
         │  └─ return: STRING (primitive)
         │
         └─ parseType("Optional<int[]>[]")
            ├─ recognize: ...[] (array)
            ├─ extract element type: Optional<int[]>
            └─ return: ARRAY(parseType("Optional<int[]>"))
               │
               └─ parseType("Optional<int[]>")
                  ├─ recognize: Optional<...>
                  ├─ extract type parameter: int[]
                  └─ return: OPTIONAL(parseType("int[]"))
                     │
                     └─ parseType("int[]")
                        ├─ recognize: ...[] (array)
                        ├─ extract element type: int
                        └─ return: ARRAY(parseType("int"))
                           │
                           └─ parseType("int")
                              └─ return: INTEGER (int)
```

### Type Environment (Γ) at Each Step

```
Γ₀: { current: "List<Map<String, Optional<int[]>[]>>" }
Γ₁: { current: "Map<String, Optional<in[]>[]>", parent: List }
Γ₂: { current: "String", parent: Map.key }
Γ₃: { current: "Optional<int[]>[]", parent: Map.value }
Γ₄: { current: "Optional<int[]>", parent: Array }
Γ₅: { current: "int[]", parent: Optional }
Γ₆: { current: "int", parent: Array }
```

### Tree Visualization

Here is it with labels:

```mermaid
graph TD
    A["LIST<...>"] --> B["MAP<K,V>"]
    B -->|key| C["String"]
    B -->|value| D["ARRAY[...]"]
    D -->|element| E["OPTIONAL<...>"]
    E -->|wrapped| F["ARRAY[...]"]
    F -->|element| G["int"]
    classDef container fill: #4a5568, stroke: #2d3748, stroke-width: 2px, color: #e2e8f0
    classDef primitive fill: #718096, stroke: #4a5568, stroke-width: 2px, color: #e2e8f0
class A, B, D, E, F container
class C,G primitive
```

### Buffer Allocation and maxSizeOf Strategy

**The maxSizeOf method is an optional hot path**. While serialize/deserialize are always on the hot path, maxSizeOf
usage depends on the application's buffer allocation strategy:

1. **Fixed Buffer Pools**: Applications with predictable message sizes can pre-allocate fixed-size buffers and reuse
   them. These applications never call maxSizeOf.

2. **Dynamic Allocation**: Applications with variable message sizes (following power-law distributions where most
   messages are <1KB but occasional "monster" messages exist) benefit from maxSizeOf to avoid:
    - Pre-allocating monster buffers for all messages (wastes memory, increases GC pressure)
    - Buffer overflow exceptions from undersized allocations

3. **Performance Consideration**: When the optional maxSizeOf is used in order to keep it fast makes a pessimistic
   estimate of the maximum size. This avoids some runtime techniques such as ZipZag encoding of `Long[]` may have a
   dramatic impact on the size used.

End.

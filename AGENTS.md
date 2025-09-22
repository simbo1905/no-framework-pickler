# AI Coding Agents for No Framework Pickler

This document provides comprehensive guidelines for AI coding agents working on the No Framework Pickler project. It complements the existing coding standards and focuses on agent-specific best practices.

## Project Context

No Framework Pickler is a Java 21+ serialization library that generates type-safe, fast serializers for records and sealed interface hierarchies using Data-Oriented Programming (DOP) principles. The project emphasizes:

- Zero dependencies and zero annotations
- Multi-stage programming with compile-time AST construction
- Performance through direct method handles
- Type safety through static semantic analysis
- Modern Java features (records, sealed interfaces, pattern matching)

## Core Agent Principles

### 1. Test-Driven Development (TDD)

**CRITICAL**: Always follow Red-Green-Refactor methodology:

- **Red**: Write failing tests first for new functionality
- **Green**: Implement minimal code to make tests pass
- **Refactor**: Improve code while keeping tests green
- **NEVER** disable or comment out tests for unwritten logic

```java
/// Example: Write test first, then implement
@Test
void shouldSerializeCustomValueType() {
    // Arrange - create test data
    final var original = new CustomRecord(customValue);
    final var pickler = Pickler.forClass(CustomRecord.class, customHandlers);
    
    // Act - serialize and deserialize
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    pickler.serialize(buffer, original);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    
    // Assert - verify correctness
    assertEquals(original, deserialized);
}
```

### 2. Data-Oriented Programming Focus

**Separate data from behavior**:

- Use `record` types for immutable data structures
- Use `sealed interface` for protocols and type hierarchies
- Prefer static methods over instance methods
- Never create utility classes - use static methods in appropriate contexts

```java
/// Good: Data + behavior separation
public record SerializationResult(ByteBuffer buffer, int bytesWritten) {}

public static SerializationResult serialize(Pickler<?> pickler, Object data) {
    // Static method provides behavior
}

/// Bad: Traditional OOP approach
public class SerializationHelper {
    private ByteBuffer buffer; // Mutable state
    public void serialize(Object data) { ... } // Instance method
}
```

### 3. Modern Java Features Usage

**Leverage Java 21+ capabilities**:

```java
/// Use pattern matching and exhaustive switches
static TypeExpr analyze(Type type) {
    return switch (type) {
        case ParameterizedType(var rawType, var args) -> 
            analyzeParameterized(rawType, args);
        case GenericArrayType(var componentType) -> 
            new ArrayNode(analyze(componentType));
        case Class<?> clazz when clazz.isArray() -> 
            new ArrayNode(analyze(clazz.getComponentType()));
        case Class<?> clazz -> analyzeClass(clazz);
        default -> throw new IllegalArgumentException("Unsupported type: " + type);
    };
}
```

## Coding Standards for Agents

### 1. Documentation Standards

**Use JEP 467 Markdown documentation comments** - never legacy JavaDoc:

```java
/// Analyzes the type structure of a Java Type to build an AST representation.
/// 
/// This method performs recursive descent parsing of Java's Type hierarchy to construct
/// Abstract Syntax Trees (AST) that represent nested container structures.
/// 
/// **Supported containers:**
/// - Arrays: `int[]`, `String[]`
/// - Lists: `List<T>`
/// - Optionals: `Optional<T>`  
/// - Maps: `Map<K,V>`
/// 
/// @param type the Java Type to analyze
/// @return TypeExpr AST node representing the type structure
/// @throws IllegalArgumentException if type is unsupported
static TypeExpr analyze(Type type) {
    // Implementation
}
```

### 2. Package Structure and Visibility

**Default to package-private scope**:

```java
/// Package-private by default - no explicit modifier
record TypeAnalysisResult(TypeExpr expr, Map<Class<?>, Long> signatures) {}

/// Static methods are package-private by default
static TypeAnalysisResult analyzeType(Type type) {
    // Implementation
}

/// Only use public for cross-package APIs
public static Pickler<T> forClass(Class<T> type) {
    Objects.requireNonNull(type, "type must not be null");
    // Implementation
}
```

### 3. Constants and Magic Numbers

**Never use magic numbers** - always use enum constants:

```java
/// Good: Use enum constants for wire protocol markers
enum Constants {
    BOOLEAN(-2),
    BYTE(-3),
    INTEGER(-4);
    
    private final int wireMarker;
    
    Constants(int wireMarker) {
        this.wireMarker = wireMarker;
    }
    
    int wireMarker() { return wireMarker; }
}

/// Usage
ZigZagEncoding.putInt(buffer, Constants.BOOLEAN.wireMarker());

/// Bad: Magic numbers
ZigZagEncoding.putInt(buffer, -2); // What does -2 mean?
```

### 4. Functional Programming Style

**Use Stream API and avoid imperative loops**:

```java
/// Good: Functional style with streams
static List<TypeExpr> analyzeComponents(RecordComponent[] components) {
    return Arrays.stream(components)
        .map(RecordComponent::getGenericType)
        .map(TypeAnalyzer::analyze)
        .collect(toList());
}

/// Bad: Imperative loops
static List<TypeExpr> analyzeComponents(RecordComponent[] components) {
    final var results = new ArrayList<TypeExpr>();
    for (RecordComponent component : components) {
        results.add(analyze(component.getGenericType()));
    }
    return results;
}
```

### 5. Error Handling and Validation

**Use appropriate validation strategies**:

```java
/// Public API: Use Objects.requireNonNull
public static Pickler<T> forClass(Class<T> type) {
    Objects.requireNonNull(type, "type must not be null");
    return createPickler(type);
}

/// Internal methods: Use assert for invariants
static TypeExpr analyzeInternal(Type type) {
    assert type != null : "type should not be null at this point";
    assert !(type instanceof WildcardType) : "wildcards not supported: " + type;
    // Implementation
}
```

## Agent-Specific Guidelines

### 1. Context Understanding

Before making changes:

1. **Read related documentation** (ARCHITECTURE.md, BACKWARDS_COMPATIBILITY.md)
2. **Understand the AST construction** - this is core to the library
3. **Review existing tests** to understand expected behavior
4. **Consider performance implications** - avoid reflection on hot paths

### 2. File Organization

**Follow the package-by-feature structure**:

- `Pickler.java` - Main public API
- `TypeExpr.java` - AST node definitions
- `Serdes.java` - Serialization/deserialization logic
- `Constants.java` - Wire protocol constants
- `CompatibilityMode.java` - Backwards compatibility handling

### 3. Testing Strategy

**Write comprehensive tests**:

```java
/// Test the happy path
@Test 
void shouldSerializeSimpleRecord() {
    // Test implementation
}

/// Test edge cases
@Test
void shouldHandleEmptyOptional() {
    // Test implementation  
}

/// Test error conditions
@Test
void shouldThrowOnUnsupportedType() {
    assertThrows(IllegalArgumentException.class, 
        () -> Pickler.forClass(UnsupportedClass.class));
}
```

### 4. Performance Considerations

**Remember the two-stage design**:

- **Meta-stage (Construction)**: Expensive reflection, AST building - acceptable cost
- **Object-stage (Runtime)**: Hot path - avoid reflection, map lookups, dynamic dispatch

```java
/// Good: Expensive work done at construction time
static Pickler<T> createPickler(Class<T> type) {
    // Expensive reflection and analysis here is OK
    final var methodHandles = analyzeAndCreateHandles(type);
    
    // Return optimized runtime implementation
    return new OptimizedPickler<>(methodHandles);
}

/// The returned pickler should avoid expensive operations
class OptimizedPickler<T> implements Pickler<T> {
    private final MethodHandle constructor;
    private final MethodHandle[] accessors;
    
    @Override
    public void serialize(ByteBuffer buffer, T obj) {
        // Fast path - no reflection, no map lookups
    }
}
```

### 5. Logging Guidelines

**Use java.util.logging with appropriate levels**:

```java
static final Logger LOGGER = Logger.getLogger(ClassName.class.getName());

/// Use lambda logging for performance
LOGGER.fine(() -> "Analyzing type: " + type.getTypeName());

/// Use different levels appropriately
LOGGER.info("Pickler created for type: " + type.getSimpleName());
LOGGER.fine(() -> "AST construction complete: " + astNode.toTreeString());
LOGGER.finer(() -> "Method handle resolved: " + methodHandle);
```

### 6. Singleton Pattern (Modern Java)

**Use sealed interfaces instead of traditional singletons**:

```java
/// Modern companion object pattern
public sealed interface TypeAnalyzer permits TypeAnalyzer.Config {
    
    record Config(boolean strictMode, Set<Class<?>> customTypes) implements TypeAnalyzer {}
    
    static TypeExpr analyze(Type type) {
        return analyze(type, new Config(true, Set.of()));
    }
    
    static TypeExpr analyze(Type type, Config config) {
        // Implementation using config
    }
}
```

## Common Anti-Patterns to Avoid

### 1. Traditional OOP Anti-Patterns

```java
/// Bad: Utility class with static methods
public class SerializationUtils {
    private SerializationUtils() {} // Pointless private constructor
    
    public static void serialize(...) { ... }
}

/// Bad: Mutable data classes
public class MutableRecord {
    private String value;
    public void setValue(String value) { this.value = value; }
}
```

### 2. Imperative Style Anti-Patterns

```java
/// Bad: Complex if-else chains
if (type instanceof List) {
    // Handle list
} else if (type instanceof Map) {
    // Handle map  
} else if (type instanceof Optional) {
    // Handle optional
} // ... many more conditions

/// Good: Exhaustive pattern matching
return switch (type) {
    case List<?> list -> analyzeList(list);
    case Map<?, ?> map -> analyzeMap(map);
    case Optional<?> opt -> analyzeOptional(opt);
    // Compiler ensures exhaustiveness
};
```

### 3. Performance Anti-Patterns

```java
/// Bad: Reflection on hot path
public void serialize(ByteBuffer buffer, Object obj) {
    Method[] methods = obj.getClass().getMethods(); // Expensive!
    // Use reflection to serialize
}

/// Good: Pre-computed method handles
public void serialize(ByteBuffer buffer, T obj) {
    // Use pre-computed direct method handles
    accessor1.invoke(obj); // Fast direct call
}
```

## Agent Success Criteria

A successful AI coding agent working on this project will:

1. **Maintain type safety** - Never compromise the static guarantees
2. **Follow TDD religiously** - Red-Green-Refactor always
3. **Use modern Java idioms** - Records, sealed interfaces, pattern matching
4. **Preserve performance characteristics** - Keep hot paths fast
5. **Write comprehensive documentation** - Using JEP 467 Markdown format
6. **Follow DOP principles** - Separate data from behavior
7. **Use functional style** - Streams, immutability, pure functions
8. **Never use magic numbers** - Always use symbolic constants

## Additional Resources

- [CODING_STYLE_LLM.md](CODING_STYLE_LLM.md) - Detailed coding standards
- [ARCHITECTURE.md](ARCHITECTURE.md) - Technical architecture overview  
- [BACKWARDS_COMPATIBILITY.md](BACKWARDS_COMPATIBILITY.md) - Schema evolution rules
- [README.md](README.md) - Project overview and usage examples

Remember: This is a performance-senstive serialization library that should feel like "just Java" to its users while providing safety and speed through careful engineering.

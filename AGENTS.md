# AI Agents Guide for No Framework Pickler

## Project Overview

**No Framework Pickler** is a type-safe, zero-dependency Java 21+ serialization library that leverages modern Data-Oriented Programming techniques. The library generates fast, compact serializers for records and sealed interface hierarchies without annotations, build-time steps, or configuration files.

## AI Agent Guidelines

When working on this repository, AI agents should understand and follow these key principles:

### Core Technical Philosophy

- **Data-Oriented Programming (DOP)**: Use Records for data, sealed interfaces for protocols, and static methods for behavior
- **Modern Java Features**: Leverage Java 21+ features including pattern matching, switch expressions, and JEP 467 markdown documentation
- **Test-Driven Development**: All code must include targeted unit tests using Red-Green-Refactor methodology
- **Type Safety**: The library provides compile-time type safety through Abstract Syntax Tree construction and static semantic analysis

### Code Standards

Agents must follow the [CODING_STYLE_LLM.md](CODING_STYLE_LLM.md) guidelines:

- **Documentation**: Use JEP 467 markdown documentation (`/// comments`) not legacy JavaDoc (`/** */`)
- **Functional Style**: Prefer Stream operations over traditional loops, exhaustive switch expressions over if-else chains
- **Package Structure**: Default to package-private scope, use public only for cross-package APIs
- **Constants**: Never use magic numbers - always use enum constants and exhaustive switches
- **Logging**: Use `java.util.logging.Logger` with appropriate levels (FINE, FINER, INFO, WARNING, SEVERE)

### Repository-Specific Context

#### Architecture Understanding
- **Multi-stage Programming**: The library uses meta-stage construction (`Pickler.forClass()`) to build ASTs and runtime serializers
- **Type Analysis**: Recursive descent parsing of Java's Type hierarchy to construct serialization logic
- **Performance**: Optimized for hot-path performance using unreflected method handles and direct delegation chains

#### Schema Evolution
- **Backwards Compatibility**: Understand the [BACKWARDS_COMPATIBILITY.md](BACKWARDS_COMPATIBILITY.md) modes (DISABLED by default, ENABLED opt-in)
- **Safe Evolution**: Only append fields to records, never reorder existing components
- **Type Signatures**: The library uses SHA256 hashes for type validation

#### Testing Strategy
- **Maven Build**: Use `./mvn-test-no-boilerplate.sh` for focused testing with minimal output
- **Test Focus**: Create exhaustive test cases for type structures using the grammar defined in [ARCHITECTURE.md](ARCHITECTURE.md)
- **Property-based Testing**: The project uses jqwik for comprehensive type combination testing

### Common Tasks and Approaches

#### Adding New Features
1. **Start with Tests**: Write failing tests that define the expected behavior
2. **Type Analysis**: Consider impact on the AST grammar and type resolution
3. **Performance**: Ensure new features don't add runtime reflection or hot-path overhead
4. **Documentation**: Update relevant .md files with clear examples

#### Bug Fixes
1. **Root Cause**: Understanding the multi-stage compilation approach is key to debugging
2. **Test Coverage**: Add regression tests that would have caught the bug
3. **Compatibility**: Consider impact on existing serialized data

#### Refactoring
1. **Functional Style**: Convert imperative loops to Stream operations where appropriate
2. **Pattern Matching**: Use exhaustive switch expressions with Records and sealed interfaces
3. **Type Safety**: Maintain compile-time guarantees throughout refactoring

### Example Patterns

#### Good: Modern Java DOP Pattern
```java
/// Process type expressions using exhaustive pattern matching
public static int calculateSize(TypeExpr expr) {
  return switch (expr) {
    case PrimitiveValueNode(var type, var javaType) -> type.sizeInBytes();
    case RefValueNode(var type, var javaType) -> type.maxSizeInBytes();
    case ArrayNode(var element, var componentType) -> 
      ARRAY_HEADER_SIZE + calculateSize(element) * DEFAULT_ARRAY_SIZE;
    case ListNode(var element) -> 
      LIST_HEADER_SIZE + calculateSize(element) * DEFAULT_LIST_SIZE;
    // ... other cases
  };
}
```

#### Avoid: Traditional OOP Patterns
```java
// Don't do this - violates DOP principles
public abstract class TypeExpr {
  public abstract int calculateSize(); // behavior mixed with data
}
```

### Integration with External Tools

#### IDE Setup
- **Java 21+**: Ensure your development environment supports modern Java features
- **Pattern Matching**: Configure IDE to recognize exhaustive switch expressions
- **Testing**: Use IDE integrations that support jqwik property-based testing

#### CI/CD Considerations  
- **Build Requirements**: Java 21+ runtime required for compilation
- **Test Execution**: Consider memory requirements for exhaustive type combination tests
- **Documentation**: Ensure markdown rendering supports JEP 467 documentation comments

### Performance Considerations

When making changes, consider:
- **Hot Path Impact**: Serialize/deserialize operations are performance-critical
- **Memory Allocation**: Minimize object creation during serialization
- **Method Handle Caching**: Understand how unreflected method handles are cached and reused
- **Buffer Management**: ByteBuffer operations should be efficient and type-safe

### Debugging Tips

- **Logging Levels**: Use system properties to control log verbosity during development
  ```bash
  mvn test -Dtest=MyTest -Djava.util.logging.ConsoleHandler.level=FINER
  ```
- **AST Visualization**: Use `TypeExpr.toTreeString()` to debug type structure issues
- **Type Signatures**: Use `pickler.typeSignature(MyRecord.class)` to understand compatibility hashes

### Non-Goals

Agents should **not** attempt to:
- Add external dependencies (library is zero-dependency by design)
- Introduce annotations or reflection on hot paths  
- Support non-record, non-sealed interface user types
- Implement general-purpose serialization beyond the defined scope
- Compromise type safety for convenience

### Questions and Escalation

When uncertain about architectural decisions:
1. Review existing patterns in the codebase first
2. Check if [ARCHITECTURE.md](ARCHITECTURE.md) covers the use case
3. Ensure changes align with Data-Oriented Programming principles
4. Consider impact on the multi-stage compilation approach

Understanding this context will help AI agents make appropriate technical decisions that align with the project's philosophy and maintain its performance characteristics.
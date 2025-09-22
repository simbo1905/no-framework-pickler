# AI Coding Agents Guide for No Framework Pickler

This document provides guidelines for AI coding agents working on the No Framework Pickler project.

## Project Overview

No Framework Pickler is a high-performance, zero-dependency Java serialization library that leverages modern Java features including records, sealed interfaces, and pattern matching. The project follows Data-Oriented Programming (DOP) principles and requires Java 21+.

### Key Technologies
- **Java 21+**: Uses modern language features (records, sealed interfaces, pattern matching)
- **Data-Oriented Programming**: Separates data (records) from behavior (static methods)
- **Zero Dependencies**: No external runtime dependencies except JUnit for tests
- **ByteBuffer-based**: High-performance binary serialization using NIO ByteBuffers

## Development Guidelines

### Code Style and Architecture
- Follow the comprehensive guidelines in [CODING_STYLE_LLM.md](CODING_STYLE_LLM.md)
- Understand the architectural patterns described in [ARCHITECTURE.md](ARCHITECTURE.md)
- Review backward compatibility constraints in [BACKWARDS_COMPATIBILITY.md](BACKWARDS_COMPATIBILITY.md)

### Essential Principles for AI Agents

#### 1. Test-Driven Development (TDD)
- **Always write tests first** following Red-Green-Refactor methodology
- **Never disable or comment out tests** for incomplete logic
- Use targeted unit tests that validate specific functionality
- Tests must be executable and pass consistently

#### 2. Modern Java Features
- Use **records** for immutable data structures
- Use **sealed interfaces** for type-safe protocol definitions
- Implement **exhaustive switch expressions** with pattern matching
- Apply **pattern destructuring** in switch statements
- Use `final var` for local variables to enhance readability

#### 3. Documentation Standards
- Use **JEP 467 Markdown documentation** (`///`) for all public APIs
- Never use legacy JavaDoc comments (`/** ... */`)
- Provide clear examples and use cases in documentation
- Document type safety guarantees and performance characteristics

#### 4. Performance Considerations
- The library is designed for high-performance scenarios
- Avoid reflection on hot paths (use MethodHandles instead)
- Consider memory allocation patterns and GC impact
- Benchmark critical paths and validate performance claims

#### 5. Type Safety
- Leverage Java's type system for compile-time safety
- Use sealed interfaces to create closed type hierarchies
- Implement exhaustive pattern matching to handle all cases
- Validate inputs at API boundaries using `Objects.requireNonNull`

## Common Patterns in the Codebase

### Sealed Interface Hierarchies
```java
public sealed interface Animal permits Mammal, Bird, Alicorn {}
sealed interface Mammal extends Animal permits Dog, Cat {}
sealed interface Bird extends Animal permits Eagle, Penguin {}
```

### Record-based Data Structures
```java
public record Dog(String name, int age) implements Mammal {}
public record Cat(String name, boolean purrs) implements Mammal {}
```

### Exhaustive Pattern Matching
```java
return switch (animal) {
    case Dog(var name, var age) -> processdog(name, age);
    case Cat(var name, var purrs) -> processCat(name, purrs);
    case Eagle(var wingspan) -> processEagle(wingspan);
    case Penguin(var canSwim) -> processPenguin(canSwim);
    case Alicorn(var name, var powers) -> processAlicorn(name, powers);
};
```

### Functional Programming Style
- Prefer Stream API over imperative loops
- Use immutable data structures and defensive copying
- Implement pure functions without side effects
- Avoid mutable state in favor of functional transformations

## Testing Strategy

### Test Organization
- Unit tests for individual components and methods
- Integration tests for serialization round-trips
- Performance benchmarks for critical paths
- Compatibility tests for schema evolution

### Test Naming and Structure
- Follow descriptive test method names that explain the scenario
- Use Given-When-Then structure in test documentation
- Group related tests in nested test classes where appropriate
- Use parameterized tests for testing multiple scenarios

### Validation Patterns
- Test both positive and negative cases
- Verify type safety guarantees through compilation
- Validate serialization/deserialization round-trips
- Check boundary conditions and edge cases

## Working with the Build System

### Maven Configuration
- Project requires Java 21+ (configured via `maven.compiler.release`)
- Uses JUnit 5 for testing framework
- Build with `mvn clean compile test`
- Run specific tests with `-Dtest=TestClassName`

### Logging and Debugging
- Use `java.util.logging.Logger` for diagnostic output
- Enable detailed logging with system properties
- Use appropriate log levels (INFO, FINE, FINER, FINEST)
- Example: `mvn test -Djava.util.logging.ConsoleHandler.level=FINER`

## AI Agent Best Practices

### Code Analysis
1. Always examine the full context before making changes
2. Understand the type hierarchy and dependencies
3. Consider backward compatibility implications
4. Validate changes against existing test suites

### Code Generation
1. Generate code that follows the established patterns
2. Ensure generated code is idiomatic Java 21+
3. Include appropriate error handling and validation
4. Write comprehensive tests for generated functionality

### Refactoring Guidelines
1. Preserve existing API contracts unless explicitly changing them
2. Maintain or improve performance characteristics
3. Keep changes minimal and focused
4. Update documentation to reflect changes

### Security Considerations
- The library emphasizes security through type safety
- Avoid reflection on untrusted data
- Validate all external inputs at API boundaries
- Use the principle of least privilege in API design

## Resources for AI Agents

### Key Files to Review
- `src/main/java/io/github/simbo1905/no/framework/Pickler.java` - Main API
- `src/test/java/io/github/simbo1905/PublicApiDemo.java` - Usage examples  
- `ARCHITECTURE.md` - Detailed technical architecture
- `CODING_STYLE_LLM.md` - Comprehensive coding standards

### Learning the Codebase
1. Start with the public API in `Pickler.java`
2. Review the test examples in `PublicApiDemo.java`
3. Understand the type analysis system in the codebase
4. Study the serialization/deserialization patterns

### Contributing Effectively
1. Follow the established patterns and conventions
2. Write tests that validate your understanding
3. Document new functionality clearly
4. Consider the impact on overall system architecture

---

*This document should be updated as the project evolves and new patterns emerge.*
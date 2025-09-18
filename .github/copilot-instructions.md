# No Framework Pickler

**ALWAYS reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.**

No Framework Pickler is a zero-dependency Java 21+ serialization library that generates type-safe, fast serializers for records and sealed interface hierarchies using Data Oriented Programming techniques.

## Working Effectively

### Environment Setup
- **Java 21+ REQUIRED**: Set Java 21+ as active before any build operations:
  ```bash
  export JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64
  export PATH=$JAVA_HOME/bin:$PATH
  java -version  # Should show Java 21+
  ```

### Build and Test Commands
- **Clean compile**: `mvn clean compile -q` -- takes ~21 seconds. NEVER CANCEL. Set timeout to 60+ minutes.
- **Single test**: `mvn test -Dtest=TestClassName -q` -- takes ~9 seconds. NEVER CANCEL. Set timeout to 30+ minutes.  
- **Full test suite**: `mvn test -q` -- takes ~4 seconds. NEVER CANCEL. Set timeout to 30+ minutes.
- **Integration tests**: `mvn verify -q` -- takes ~67 seconds. NEVER CANCEL. Set timeout to 120+ minutes.
- **Use the helper script**: `./mvn-test-no-boilerplate.sh -Dtest=TestClassName` (strips Maven boilerplate output)
  - **NOTE**: The script uses `mvnd` (Maven Daemon) which may not be installed. If it fails, manually replace `mvnd test` with `mvn test` in the script.
- **Test with logging**: `mvn test -Dtest=TestClassName -Djava.util.logging.ConsoleHandler.level=FINER`

### Demo and Validation
- **Run the public API demo**: 
  ```bash
  mvn test-compile exec:java -Dexec.mainClass="io.github.simbo1905.PublicApiDemo" -Dexec.classpathScope="test" -q
  ```
- **Use Maven exec for utilities**: For any utility classes that need project dependencies:
  ```bash
  mvn exec:java -Dexec.mainClass="org.sample.UtilityClass" -Dexec.classpathScope="test" -q
  ```

### Validation Scenarios
After making changes, ALWAYS run these validation steps:
1. **Compile validation**: `mvn clean compile -q` must succeed
2. **Core test validation**: `mvn test -Dtest=CoreValueTypesTest -q` must pass
3. **Full test suite**: `mvn test -q` must pass (all ~32 test files)
4. **Integration tests**: `mvn verify -q` must pass (includes property-based testing)
5. **Demo execution**: Run the PublicApiDemo to validate serialization works end-to-end

## Codebase Structure

### Key Projects and Locations
- **Main library code**: `src/main/java/io/github/simbo1905/no/framework/`
  - `Pickler.java` - Main public API entry point
  - `Serde.java`, `RecordSerde.java`, `EnumSerde.java` - Core serialization logic
  - `ComponentSerde.java` - Component-level serialization
  - `ZigZagEncoding.java` - Varint encoding (from HdrHistogram)
- **Test code**: `src/test/java/io/github/simbo1905/no/framework/`
  - `CoreValueTypesTest.java` - Basic value type testing
  - `UserTypesTest.java` - Records, enums, interfaces, recursive structures
  - `ITExhaustiveTest.java` - Property-based exhaustive testing
  - `BackwardsCompatibilityTests.java` - Schema evolution testing
- **Demo code**: `src/test/java/io/github/simbo1905/PublicApiDemo.java`
- **Documentation**: `README.md`, `ARCHITECTURE.md`, `BACKWARDS_COMPATIBILITY.md`
- **Coding standards**: `CODING_STYLE_LLM.md`

### Important Build Files
- **Maven config**: `pom.xml` (requires Java 21, includes JUnit 5, AssertJ, jqwik)
- **Test helper**: `mvn-test-no-boilerplate.sh` (strips Maven output, use `mvn` not `mvnd`)
- **GitHub Actions**: `.github/workflows/maven.yml` (uses `mvn -B verify`)

## Development Guidelines

### Testing Philosophy
- **TDD approach**: All code must include targeted unit tests
- **Never disable tests**: Follow Red-Green-Refactor coding
- **Property-based testing**: Use jqwik for exhaustive testing (see ITExhaustiveTest)
- **Manual validation required**: Always test actual functionality, not just start/stop

### Logging and Debugging
- **Use Java's built-in logging**: `java.util.logging.Logger`
- **Log levels**: FINE (production debugging), FINER (verbose), INFO (important runtime)
- **Test with verbose logs**: `mvn test -Dtest=TestClass -Djava.util.logging.ConsoleHandler.level=FINER`
- **Enable in tests**: System property `java.util.logging.ConsoleHandler.level=FINER`

### Code Style Requirements
- **Data-Oriented Programming**: Separate immutable data (Records) from behavior (static methods)
- **Package-private by default**: Use default access, limit public to cross-package APIs
- **Records must be public**: NFP requires public records for reflection access
- **Modern Java features**: Use Java 21+ features (records, pattern matching, sealed classes)
- **Assertions**: Use `assert` for internal validation, `Objects.requireNonNull` for public API

### Schema Evolution and Compatibility
- **Default mode**: DISABLED (strict, no backwards compatibility)
- **Compatibility mode**: Set system property `no.framework.Pickler.Compatibility=ENABLED`
- **Backwards compatibility rules** (when enabled):
  - **Only append** new fields to end of records
  - **Never reorder** existing fields (causes silent corruption)
  - **Component renaming works** (unlike JDK serialization)

## Common Commands Reference

### Build Timing Expectations
- **Clean compile**: ~21 seconds - NEVER CANCEL, set 60+ minute timeout
- **Single test**: ~9 seconds - NEVER CANCEL, set 30+ minute timeout  
- **Full test suite**: ~4 seconds - NEVER CANCEL, set 30+ minute timeout
- **Integration tests**: ~67 seconds - NEVER CANCEL, set 120+ minute timeout

### Frequently Used Test Commands
```bash
# Run single test class with logging
mvn test -Dtest=CoreValueTypesTest -Djava.util.logging.ConsoleHandler.level=FINER

# Run specific test method
mvn test -Dtest=UserTypesTest#testRecordTypes

# Run multiple test classes  
mvn test -Dtest=CoreValueTypesTest,UserTypesTest

# Use helper script (strips Maven boilerplate)
./mvn-test-no-boilerplate.sh -Dtest=RefactorTests -Djava.util.logging.ConsoleHandler.level=FINER
```

### Maven Profiles
- **Default profile**: Strict compilation with `-Werror`
- **Relaxed profile**: `mvn test -P relaxed` (no -Werror, used by helper script)

## Architecture and Design

### Core Design Principles
- **Multi-stage programming**: Meta-stage during `Pickler.forClass()` construction, runtime without reflection
- **AST construction**: Recursive descent parsing of Java Type hierarchy
- **Linear dependency optimization**: Automatic 2x performance improvement for non-circular type hierarchies
- **Type safety**: All reflective operations done at construction time, runtime uses method handles

### Type System Grammar
```
TypeExpression ::= ValueType | ContainerExpression
ContainerExpression ::= ARRAY(TypeExpression) | LIST(TypeExpression) | OPTIONAL(TypeExpression) | MAP(TypeExpression, TypeExpression)
ValueType ::= PrimitiveType | ReferenceType
```

Examples: `List<Optional<String[]>>` → `LIST(OPTIONAL(ARRAY(String)))`

### Performance Characteristics
- **Linear hierarchies**: 2x faster lookups (automatic optimization)
- **Circular hierarchies**: Standard performance (automatic fallback)
- **Binary payload**: 0.5x size compared to JDK Serialization
- **Type-safe**: No ClassCastException risks, resolved at construction time

## Error Handling

### Common Issues and Solutions
- **Java version**: Must use Java 21+, check with `java -version`
- **mvnd not found**: The helper script uses Maven Daemon (`mvnd`) which may not be installed. Fix by running:
  ```bash
  sed 's/mvnd test/mvn test/g' mvn-test-no-boilerplate.sh > /tmp/mvn-test-fixed.sh
  chmod +x /tmp/mvn-test-fixed.sh
  # Then use: /tmp/mvn-test-fixed.sh -Dtest=TestClassName
  ```
- **Compilation errors**: Use relaxed profile: `mvn test -P relaxed`
- **Demo not found**: Must compile tests first: `mvn test-compile` before running demo
- **Long build times**: This is normal, builds can take 45+ minutes, NEVER CANCEL

### Validation Checklist
Before completing any change:
- [ ] Code compiles: `mvn clean compile -q`
- [ ] Core tests pass: `mvn test -Dtest=CoreValueTypesTest -q`  
- [ ] Full test suite passes: `mvn test -q`
- [ ] Integration tests pass: `mvn verify -q` 
- [ ] Demo runs successfully: Run PublicApiDemo
- [ ] Manual functionality tested: Exercise actual serialization/deserialization scenarios

**CRITICAL**: NEVER CANCEL builds or tests. They may take 45+ minutes but must complete fully.
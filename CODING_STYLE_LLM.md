# Java DOP Coding Standards ####################

This file is a Gen AI summary of CODING_STYLE.md to use less tokens of context window. Read the original file for full details.

IMPORTANT: We do TDD so all code must include targeted unit tests.
IMPORTANT: Never disable tests written for logic that we are yet to write we do Red-Green-Refactor coding. 

## Core Principles

* Use Records for all data structures. Use sealed interfaces for protocols.
* Prefer static methods with Records as parameters
* Default to package-private scope
* Package-by-feature, not package-by-layer
* Create fewer, cohesive, wide packages (functionality modules or records as protocols)
* Use public only when cross-package access is required
* Use JEP 467 Markdown documentation examples: `/// good markdown` not legacy `/** bad html */`
* Apply Data-Oriented Programming principles and avoid OOP
* Use Stream operations instead of traditional loops. Never use `for(;;)` with mutable loop variables use
  `Arrays.setAll`
* Prefer exhaustive destructuring switch expressions over if-else statements
* Use destructuring switch expressions that operate on Records and sealed interfaces
* Use anonymous variables in record destructuring and switch expressions
* Use `final var` for local variables, parameters, and destructured fields
* Apply JEP 371 "Local Classes and Interfaces" for cohesive files with narrow APIs

## Data-Oriented Programming

* Separate data (immutable Records) from behavior (never utility classes always static methods)
* Use immutable generic data structures (maps, lists, sets) and take defense copies in constructors
* Write pure functions that don't modify state
* Leverage Java 21+ features:
    * Records for immutable data
    * Pattern matching for structural decomposition
    * Sealed classes for exhaustive switches
    * Virtual threads for concurrent processing

## Package Structure

* Use default (package-private) access as the standard. Do not use 'private' or 'public' by default.
* Limit public to genuine cross-package APIs
* Prefer package-private static methods. Do not use 'private' or 'public' by default.
* Limit private to security-related code
* Avoid anti-patterns: boilerplate OOP, excessive layering, dependency injection overuse

## Constants and Magic Numbers

* **NEVER use magic numbers** - always use enum constants
* **NEVER write large if-else-if statements over known types** - will not be exhaustive and creates bugs when new types are added. Use exhaustive switch statements over bounded sets such as enum values or sealed interface permits
* **Wire protocol markers**: Use `Constants.TYPE.wireMarker()` not hardcoded negative numbers like `-2`
* **Type markers**: Use `Constants.TYPE.marker()` not hardcoded positive numbers like `2`
* **Type lookups**: Use `Constants.fromMarker(byte)` for reverse lookups
* **Examples**: 
  * ❌ `ZigZagEncoding.putInt(buffer, -2)` 
  * ✅ `ZigZagEncoding.putInt(buffer, Constants.BOOLEAN.wireMarker())`

## Functional Style

* Combine Records + static methods for functional programming
* Emphasize immutability and explicit state transformations
* Reduce package count to improve testability
* Implement Algebraic Data Types pattern with Function Modules
* Modern Stream Programming
* Use Stream API instead of traditional loops
* Write declarative rather than imperative code
* Chain operations without intermediate variables
* Support immutability throughout processing
* Example: `IntStream.range(0, 100).filter(i -> i % 2 == 0).sum()` instead of counting loops
* Always use final variables in functional style. 
* Prefer `final var` with self documenting names over `int i` or `String s` but its not possible to do that on a `final` variable that is not yet initialized so its a weak preference not a strong one.
* Avoid just adding new functionality to the top of a method to make an early return. It is fine to have a simple guard statement. Yet general you should pattern match over the input to do different things with the same method. Adding special case logic is a code smell that should be avoided.

## Documentation using JEP 467 Markdown documentation

IMPORTANT: You must not write JavaDoc comments that start with `/**` and end with `*/`
IMPORTANT: You must "JEP 467: Markdown Documentation Comments" that start all lines with `///`

Here is an example of the correct format for documentation comments:

```java
/// Returns a hash code value for the object. This method is
/// supported for the benefit of hash tables such as those provided by
/// [java.util.HashMap].
///
/// The general contract of `hashCode` is:
///
///   - Whenever it is invoked on the same object more than once during
///     an execution of a Java application, the `hashCode` method
///   - If two objects are equal according to the
///     [equals][#equals(Object)] method, then calling the
///   - It is _not_ required that if two objects are unequal
///     according to the [equals][#equals(Object)] method, then
///
/// @return a hash code value for this object.
/// @see     java.lang.Object#equals(java.lang.Object)
```

## Logging

- Use Java's built-in logging: `java.util.logging.Logger`
- Log levels: Use appropriate levels (FINE, FINER, INFO, WARNING, SEVERE)
  - **FINE**: Production-level debugging, default for most debug output
  - **FINER**: Verbose debugging, detailed internal flow, class resolution details
  - **INFO**: Important runtime information
- LOGGER is a static field: `static final Logger LOGGER = Logger.getLogger(ClassName.class.getName());`
- Use lambda logging for performance: `LOGGER.fine(() -> "message " + variable);`
- **Testing with Verbose Logs**: Use system property override in test commands:
  ```bash
  mvn test -Dtest=MachineryTests#testMethod -Djava.util.logging.ConsoleHandler.level=FINER
  ```

## Maven Utilities and Scripts

**For project utilities that need dependencies**: Use Maven exec instead of complex classpath management:
```bash
# Run utility classes with all project dependencies
mvn exec:java -Dexec.mainClass="org.sample.UtilityClass" -q

# Use token-saving script to reduce Maven output
./mvn-test-no-boilerplate.sh exec:java -Dexec.mainClass="org.sample.UtilityClass"
```

**Critical Lessons from Benchmarking**:
- **Measure ACTUAL test data**: Import real benchmark records, don't create fake copies that fall out of date
- **Use Maven exec for utilities**: Avoid complex classpath setup when project dependencies are needed
- **Records must be public**: NFP requires public records for reflection access


## Modern Java Singleton Pattern: Sealed Interfaces

**Anti-Pattern**: Traditional singleton classes with private constructors and static instances are legacy should be avoided.

**Modern Pattern**: Use sealed interfaces, with nested  with static methods that only has a nested record that is the configuration object to use with 
the default methods

### Implementation

```java
/// Modern Java companion object pattern avoiding singleton anti-patterns
public sealed interface LoggingControl permits LoggingControl.Config {

  /// Configuration record for immutable state
  record Config(Level defaultLevel) implements LoggingControl {}
  
  /// Static methods provide functionality without instantiation
  static void setupCleanLogging(Config config) {
    // Implementation here - no instances required
  }
  
  /// Convenience methods with sensible defaults
  static void setupCleanLogging() {
    setupCleanLogging(new Config(Level.WARNING));
  }
}
```

### Benefits

1. **No instantiation possible**: Interface cannot be constructed directly
2. **Functional style**: Static methods provide clean API without state
3. **Type safety**: Sealed interface with permits controls allowed implementations  
4. **Configuration via records**: Immutable configuration objects instead of mutable state
5. **Modern Java idioms**: Uses features introduced in Java 17+ (sealed types, records)

### Usage

```java
// Clean functional calls - no instances, no singletons
LoggingControl.setupCleanLogging(); // Uses default config
LoggingControl.setupCleanLogging(new LoggingControl.Config(Level.FINER)); // Custom config
```

This pattern replaces traditional singleton anti-patterns with modern, functional Java that is easier to test, reason about, and maintain.

### Assertions and Input Validation

1.  On the public API entry points use `Objects.assertNonNull()` to ensure that the inputs are legal.
  - e.g. the input of  `Pickler.forClass(Class<T> type)` is immediate checked for null.
2. After that on internal method that should be passed only valid data use `assert` to ensure that the data is valid.
  - e.g. use `assert x==y: "unexpected x="+x+" y="+y;` as `mvn` base should be run with `-ea` to enable assertions.
3. Often there is an `orElseThrow()` which can be used so the only reason to use `assert` is to add more logging to the error message.
4. Consider using the validations of `Object` and `Arrays` ane the like to ensure that the data is valid.
  - e.g. `Objects.requireNonNull(type, "type must not be null")` or `Arrays.checkIndex(index, array.length)`.

## Case Study 1: 

⏺ Good. The refactoring is complete and uses proper Optional handling instead of special case checks.

> see! the flag structure of
82      // Check built-in types last
83      for (Tag tag : values()) {
84        if (tag == INTERFACE) continue; // INTERFACE is handled above for sealed interfaces
85        for (Class<?> supported : tag.supportedClasses) {
86          if (supported.equals(clazz) || supported.isAssignableFrom(clazz)) {
87            return tag;
88          }
89        }
90      }
>The indentation literally implies that that the `if` is a peer of `for` when in fact the `for` is broken without the `if`. 
>With the functional stream approach
83      return Arrays.stream(values())
84          .filter(tag -> Optional.ofNullable(tag.supportedClasses)
85              .stream()
86              .flatMap(Arrays::stream)
87              .anyMatch(supported -> supported.equals(clazz) || supported.isAssignableFrom(clazz)))
88          .findFirst()
89          .orElseThrow(() -> new IllegalArgumentException("Unsupported class: " + clazz.getName()));
> it is literally impossible to not understand that the nullability or not is a specific thing. we all know that the filter/flagMap/anyMatch/findFirst is lazy and deferred it is only `orElseThrow` that is a terminal that is the point of execution. which is why CODING_STYLE_LLM.md is telling you to use functional streams.

⏺ Got it. The functional stream approach makes the nullability handling explicit and impossible to misread. The imperative code with the if (tag == INTERFACE) continue;
  creates a hidden dependency - the inner for loop will NPE without that guard, but the indentation makes them look like independent statements.

  In the stream version, Optional.ofNullable(tag.supportedClasses) makes it crystal clear that we're dealing with potentially null values, and the data flow is explicit:
  filter tags → handle nullable arrays → flatten → match → find first → throw if not found. The lazy evaluation means we only process as much as needed until we find a
  match.

  This is exactly why modern Java prefers streams - they force you to handle edge cases explicitly in the data flow rather than hiding them in control flow that can be
  easily misread or refactored incorrectly.


## JEP References

[JEP 467](https://openjdk.org/jeps/467): Markdown Documentation in JavaDoc
[JEP 371](https://openjdk.org/jeps/371): Local Classes and Interfaces
[JEP 395](https://openjdk.org/jeps/395): Records
[JEP 409](https://openjdk.org/jeps/409): Sealed Classes
[JEP 440](https://openjdk.org/jeps/440): Record Patterns
[JEP 427](https://openjdk.org/jeps/427): Pattern Matching for Switch

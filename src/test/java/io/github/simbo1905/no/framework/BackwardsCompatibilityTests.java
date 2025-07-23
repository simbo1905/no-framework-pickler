// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.tools.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/// Tests for backwards compatibility support in No Framework Pickler.
/// This test uses dynamic compilation to test different versions of records
/// to ensure data serialized with older schemas can be read by newer versions.
/// The `ENABLED` system property must be set to allow this compatibility mode. It will then use the default values for
/// new fields that are not read back in the serialized data.
public class BackwardsCompatibilityTests {

  // Generation 0: Empty record
  static final String GENERATION_0 = """
      package io.github.simbo1905.no.framework.evolution;
      
      /// An empty record.
      public record SimpleRecord() {
      }
      """;

  // Generation 1: Original schema with just one field
  static final String GENERATION_1 = """
      package io.github.simbo1905.no.framework.evolution;
      
      /// A simple record with a single field.
      public record SimpleRecord(int value) {
      }
      """;

  // Generation 2: Evolved schema with two additional fields
  static final String GENERATION_2 = """
      package io.github.simbo1905.no.framework.evolution;
      
      /// An evolved record with additional fields.
      public record SimpleRecord(int value, String name, double score) {
      }
      """;

  final String FULL_CLASS_NAME = "io.github.simbo1905.no.framework.evolution.SimpleRecord";
  private String originalCompatibilityValue;

  /// Deferred static initialization holder for the JavaCompiler
  static class CompilerHolder {
    static final JavaCompiler COMPILER = ToolProvider.getSystemJavaCompiler();
  }

  @BeforeEach
  void setUp() {
    // Save the original property value and set the compatibility mode to ENABLED for tests
    originalCompatibilityValue = System.getProperty("no.framework.Pickler.Compatibility");
    System.setProperty("no.framework.Pickler.Compatibility", "ENABLED");
  }

  @AfterEach
  void tearDown() {
    // Restore the original property value
    if (originalCompatibilityValue != null) {
      System.setProperty("no.framework.Pickler.Compatibility", originalCompatibilityValue);
    } else {
      System.clearProperty("no.framework.Pickler.Compatibility");
    }
  }

  @Test
  void testEvolveFromOneField() throws Exception {
    setUp();
    // Compile and marshal the original schema (V1)
    Class<?> originalClass = compileAndClassLoad(FULL_CLASS_NAME, GENERATION_1);
    Object originalInstance = createRecordInstance(originalClass, new Object[]{42});
    byte[] serializedData = serializeRecord(originalInstance);

    // we need to know the type signature of the record to deserialize it correctly as the new class
    Pickler<?> pickler = Pickler.forClass(originalClass);
    final long typeSignature = pickler.typeSignature(originalClass);

    // Compile and unmarshal the evolved schema (V2)
    Class<?> evolvedClass = compileAndClassLoad(FULL_CLASS_NAME, GENERATION_2);

    Object evolvedInstance = deserializeRecord(evolvedClass, serializedData, typeSignature);

    // Verify the deserialized instance has the expected values
    Map<String, Object> expectedValues = new HashMap<>();
    expectedValues.put("value", 42);
    expectedValues.put("name", null);   // Should use null default for missing String
    expectedValues.put("score", 0.0);   // Should use 0.0 default for missing double
    verifyRecordComponents(evolvedInstance, expectedValues);
  }

  @Test
  void testEvolveFromEmptyRecord() throws Exception {
    setUp();
    // Compile and load the empty schema (V0)
    Class<?> originalClass = compileAndClassLoad(FULL_CLASS_NAME, GENERATION_0);
    Object originalInstance = createRecordInstance(originalClass, new Object[]{});
    byte[] serializedData = serializeRecord(originalInstance);


    // we need to know the type signature of the record to deserialize it correctly as the new class
    Pickler<?> pickler = Pickler.forClass(originalClass);
    final long typeSignature = pickler.typeSignature(originalClass);

    // Compile and load the evolved schema (V2)
    Class<?> evolvedClass = compileAndClassLoad(FULL_CLASS_NAME, GENERATION_2);
    Object evolvedInstance = deserializeRecord(evolvedClass, serializedData, typeSignature);

    // Verify the deserialized instance has the expected values (all defaults)
    Map<String, Object> expectedValues = new HashMap<>();
    expectedValues.put("value", 0);
    expectedValues.put("name", null);
    expectedValues.put("score", 0.0);
    verifyRecordComponents(evolvedInstance, expectedValues);
  }

  /// Helper method to compile Java source code and load the resulting class
  static Class<?> compileAndClassLoad(@SuppressWarnings("SameParameterValue") String fullClassName, String code)
      throws ClassNotFoundException {
    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
    InMemoryFileManager fileManager = new InMemoryFileManager(
        CompilerHolder.COMPILER.getStandardFileManager(diagnostics, null, null));
    JavaFileObject sourceFile = new InMemorySourceFile(fullClassName, code);
    JavaCompiler.CompilationTask task = CompilerHolder.COMPILER.getTask(
        null, fileManager, diagnostics, null, null, List.of(sourceFile));

    if (!task.call()) {
      throwCompilationError(diagnostics);
    }

    byte[] classBytes = fileManager.getClassBytes(fullClassName);
    InMemoryClassLoader classLoader = new InMemoryClassLoader(
        BackwardsCompatibilityTests.class.getClassLoader(),
        Map.of(fullClassName, classBytes));
    return classLoader.loadClass(fullClassName);
  }

  /// Creates an instance of a record using reflection
  static Object createRecordInstance(Class<?> recordClass, Object[] args) throws Exception {
    RecordComponent[] components = recordClass.getRecordComponents();
    Class<?>[] paramTypes = Arrays.stream(components)
        .map(RecordComponent::getType)
        .toArray(Class<?>[]::new);
    Constructor<?> constructor = recordClass.getDeclaredConstructor(paramTypes);
    return constructor.newInstance(args);
  }

  /// Serializes a record instance using the current Pickler API
  @SuppressWarnings({"unchecked", "rawtypes"})
  static byte[] serializeRecord(Object record) {
    Class recordClass = record.getClass();
    Pickler pickler = Pickler.forClass(recordClass);
    int size = pickler.maxSizeOf(record);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    pickler.serialize(buffer, record);
    buffer.flip();
    byte[] result = new byte[buffer.remaining()];
    buffer.get(result);
    return result;
  }

  /// Deserializes a record from bytes using the Pickler
  static Object deserializeRecord(Class<?> recordClass, byte[] bytes, long typeSignature) {
    Map<Class<?>, Long> typeSignatures = Map.of(recordClass, typeSignature);
    Pickler<?> pickler = Pickler.forClass(recordClass, typeSignatures);
    return pickler.deserialize(ByteBuffer.wrap(bytes));
  }

  /// Throws a runtime exception with compilation error details
  static void throwCompilationError(DiagnosticCollector<JavaFileObject> diagnostics) {
    StringBuilder errorMsg = new StringBuilder("Compilation failed:\n");
    diagnostics.getDiagnostics().forEach(d -> errorMsg.append(d).append("\n"));
    throw new RuntimeException(errorMsg.toString());
  }

  /// Verifies that a record instance has the expected component values
  static void verifyRecordComponents(Object record, Map<String, Object> expectedValues) {
    Class<?> recordClass = record.getClass();
    Arrays.stream(recordClass.getRecordComponents())
        .filter(c -> expectedValues.containsKey(c.getName()))
        .forEach(c -> verifyComponent(record, c, expectedValues));
  }

  /// Verifies a single record component value
  static void verifyComponent(Object record, RecordComponent component, Map<String, Object> expectedValues) {
    String name = component.getName();
    try {
      Method accessor = component.getAccessor();
      Object actualValue = accessor.invoke(record);
      Object expectedValue = expectedValues.get(name);
      assertEquals(expectedValue, actualValue, "Component '" + name + "' has value " + actualValue + " but expected " + expectedValue);
    } catch (Exception e) {
      fail("Failed to access component '" + name + "': " + e.getMessage());
    }
  }

  /// A JavaFileObject implementation that holds source code in memory
  static class InMemorySourceFile extends SimpleJavaFileObject {
    private final String code;

    InMemorySourceFile(String className, String code) {
      super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
      this.code = code;
    }

    @Override
    public CharSequence getCharContent(boolean ignoreEncodingErrors) {
      return code;
    }
  }

  /// A JavaFileObject implementation that collects compiled bytecode in memory
  static class InMemoryClassFile extends SimpleJavaFileObject {
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    InMemoryClassFile(String className) {
      super(URI.create("bytes:///" + className.replace('.', '/') + Kind.CLASS.extension), Kind.CLASS);
    }

    byte[] getBytes() {
      return outputStream.toByteArray();
    }

    @Override
    public OutputStream openOutputStream() {
      return outputStream;
    }
  }

  /// A JavaFileManager that keeps compiled classes in memory
  static class InMemoryFileManager extends ForwardingJavaFileManager<StandardJavaFileManager> {
    private final Map<String, InMemoryClassFile> classFiles = new HashMap<>();

    InMemoryFileManager(StandardJavaFileManager fileManager) {
      super(fileManager);
    }

    @Override
    public JavaFileObject getJavaFileForOutput(Location location, String className, JavaFileObject.Kind kind, FileObject sibling) {
      if (kind == JavaFileObject.Kind.CLASS) {
        InMemoryClassFile classFile = new InMemoryClassFile(className);
        classFiles.put(className, classFile);
        return classFile;
      }
      try {
        return super.getJavaFileForOutput(location, className, kind, sibling);
      } catch (IOException e) {
        throw new RuntimeException("Failed to get file for output", e);
      }
    }

    byte[] getClassBytes(String className) {
      InMemoryClassFile file = classFiles.get(className);
      if (file == null) throw new IllegalArgumentException("No class file for: " + className);
      return file.getBytes();
    }
  }

  /// A ClassLoader that loads classes from in-memory bytecode
  static class InMemoryClassLoader extends ClassLoader {
    private final Map<String, byte[]> classBytes;

    InMemoryClassLoader(ClassLoader parent, Map<String, byte[]> classBytes) {
      super(parent);
      this.classBytes = new HashMap<>(classBytes);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      byte[] bytes = classBytes.get(name);
      if (bytes != null) {
        return defineClass(name, bytes, 0, bytes.length);
      }
      return super.findClass(name);
    }
  }
}

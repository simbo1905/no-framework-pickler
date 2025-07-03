package io.github.simbo1905.no.framework;

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

import static org.junit.jupiter.api.Assertions.*;

/// Tests for backwards compatibility support in No Framework Pickler.
/// This test uses dynamic compilation to test different versions of records
/// to ensure data serialized with older schemas can be read by newer versions.
/// @deprecated This test covers a legacy opt-in compatibility mode. The modern approach,
/// as implemented in `src/main/java/io/github/simbo1905/no/framework/Pickler.java`,
/// is to be safe-by-default using cryptographic hashes to detect schema changes.
/// The recommended strategy for deliberate schema evolution is described in `ARCHITECTURE.md`.
@Deprecated
public class BackwardsCompatibilityTest {

    // Original schema with just one field
    static final String GENERATION_1 = """
        package io.github.simbo1905.no.framework.evolution;
        
        /// A simple record with a single field.
        public record SimpleRecord(int value) {
        }
        """;

    // Evolved schema with two additional fields
    static final String GENERATION_2 = """
        package io.github.simbo1905.no.framework.evolution;
        
        /// An evolved record with additional fields.
        public record SimpleRecord(int value, String name, double score) {
        }
        """;

    final String FULL_CLASS_NAME = "io.github.simbo1905.no.framework.evolution.SimpleRecord";
    
    /// Deferred static initialization holder for the JavaCompiler
    static class CompilerHolder {
        static final JavaCompiler COMPILER = ToolProvider.getSystemJavaCompiler();
    }

    @Test
    void testBackwardsCompatibilityWithSystemProperty() throws Exception {
        // Save the original property value
        String originalValue = System.getProperty("no.framework.Pickler.Compatibility");
        
        // Set the backwards compatibility system property
        System.setProperty("no.framework.Pickler.Compatibility", "DEFAULTED");
        try {
            // Compile and load the original schema
            Class<?> originalClass = compileAndClassLoad(FULL_CLASS_NAME, GENERATION_1);
            assertTrue(originalClass.isRecord(), "Compiled class should be a record");

            // Create an instance of the original record
            Object originalInstance = createRecordInstance(originalClass, new Object[]{42});

            // Serialize the original instance using current Pickler API
            byte[] serializedData = serializeRecord(originalInstance);

            // Compile and load the evolved schema
            Class<?> evolvedClass = compileAndClassLoad(FULL_CLASS_NAME, GENERATION_2);
            assertTrue(evolvedClass.isRecord(), "Evolved class should be a record");

            // With backwards compatibility enabled, this should succeed
            Object evolvedInstance = deserializeRecord(evolvedClass, serializedData);

            // Verify the deserialized instance has the expected values
            Map<String, Object> expectedValues = new HashMap<>();
            expectedValues.put("value", 42);
            expectedValues.put("name", null);   // Should use null default for missing String
            expectedValues.put("score", 0.0);   // Should use 0.0 default for missing double
            verifyRecordComponents(evolvedInstance, expectedValues);
        } finally {
            // Restore the original property value
            if (originalValue != null) {
                System.setProperty("no.framework.Pickler.Compatibility", originalValue);
            } else {
                System.clearProperty("no.framework.Pickler.Compatibility");
            }
        }
    }

    /// Helper method to compile Java source code and load the resulting class
    static Class<?> compileAndClassLoad(String fullClassName, String code) 
            throws ClassNotFoundException {
        // Set up the compilation
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        InMemoryFileManager fileManager = new InMemoryFileManager(
            CompilerHolder.COMPILER.getStandardFileManager(diagnostics, null, null));

        // Create the source file object
        JavaFileObject sourceFile = new InMemorySourceFile(fullClassName, code);

        // Compile the source
        JavaCompiler.CompilationTask task = CompilerHolder.COMPILER.getTask(
            null, fileManager, diagnostics, null, null, List.of(sourceFile));

        boolean success = task.call();
        if (!success) {
            throwCompilationError(diagnostics);
        }

        // Get the compiled bytecode
        byte[] classBytes = fileManager.getClassBytes(fullClassName);

        // Load the compiled class
        InMemoryClassLoader classLoader = new InMemoryClassLoader(
            BackwardsCompatibilityTest.class.getClassLoader(),
            Map.of(fullClassName, classBytes));

        return classLoader.loadClass(fullClassName);
    }

    /// Creates an instance of a record using reflection
    static Object createRecordInstance(Class<?> recordClass, Object[] args) throws Exception {
        // Get constructor matching the record components
        RecordComponent[] components = recordClass.getRecordComponents();
        Class<?>[] paramTypes = Arrays.stream(components)
            .map(RecordComponent::getType)
            .toArray(Class<?>[]::new);

        // Get the constructor matching the component types
        Constructor<?> constructor = recordClass.getDeclaredConstructor(paramTypes);

        // Create and return the instance
        return constructor.newInstance(args);
    }

    /// Serializes a record instance using the current Pickler API
    @SuppressWarnings({"unchecked", "rawtypes"})
    static byte[] serializeRecord(Object record) {
        // Get the pickler for the record class
        Class recordClass = record.getClass();
        Pickler pickler = Pickler.forClass(recordClass);

        // Calculate buffer size and allocate buffer
        int size = pickler.maxSizeOf(record);
        ByteBuffer buffer = ByteBuffer.allocate(size);

        // Serialize the record
        pickler.serialize(buffer, record);
        buffer.flip();

        // Return the serialized bytes
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    /// Deserializes a record from bytes using the Pickler
    static Object deserializeRecord(Class<?> recordClass, byte[] bytes) {
        Pickler<?> pickler = Pickler.forClass(recordClass);
        return pickler.deserialize(ByteBuffer.wrap(bytes));
    }

    /// Throws a runtime exception with compilation error details
    static void throwCompilationError(DiagnosticCollector<JavaFileObject> diagnostics) {
        StringBuilder errorMsg = new StringBuilder("Compilation failed:\n");
        for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
            errorMsg.append(diagnostic).append("\n");
        }
        throw new RuntimeException(errorMsg.toString());
    }

    /// Verifies that a record instance has the expected component values
    static void verifyRecordComponents(Object record, Map<String, Object> expectedValues) {
        Class<?> recordClass = record.getClass();
        RecordComponent[] components = recordClass.getRecordComponents();

        Arrays.stream(components)
            .filter(component -> expectedValues.containsKey(component.getName()))
            .forEach(component -> verifyComponent(record, component, expectedValues));
    }

    /// Verifies a single record component value
    static void verifyComponent(Object record, RecordComponent component, Map<String, Object> expectedValues) {
        String name = component.getName();
        try {
            Method accessor = component.getAccessor();
            Object actualValue = accessor.invoke(record);
            Object expectedValue = expectedValues.get(name);

            assertEquals(expectedValue, actualValue,
                "Component '" + name + "' has value " + actualValue +
                " but expected " + expectedValue);
        } catch (Exception e) {
            fail("Failed to access component '" + name + "': " + e.getMessage());
        }
    }

    /// A JavaFileObject implementation that holds source code in memory
    static class InMemorySourceFile extends SimpleJavaFileObject {
        private final String code;

        InMemorySourceFile(String className, String code) {
            super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension),
                Kind.SOURCE);
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
            super(URI.create("bytes:///" + className.replace('.', '/') + Kind.CLASS.extension),
                Kind.CLASS);
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
        public JavaFileObject getJavaFileForOutput(Location location, String className,
                                                   JavaFileObject.Kind kind, FileObject sibling) {
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
            if (file == null) {
                throw new IllegalArgumentException("No class file for: " + className);
            }
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

package io.github.simbo1905.no.framework;

import javax.tools.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public sealed interface RecordSourceCodeToClassLoadWithInstance permits RecordSourceCodeToClassLoadWithInstance.None {

  @SuppressWarnings("unused")
  enum None implements RecordSourceCodeToClassLoadWithInstance {INSTANCE}

  class CompilerHolder {
    static final JavaCompiler COMPILER = ToolProvider.getSystemJavaCompiler();
  }

  static Class<?> compileAndClassLoad(String fullClassName, String code)
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
        RecordSourceCodeToClassLoadWithInstance.class.getClassLoader(),
        Map.of(fullClassName, classBytes));
    return classLoader.loadClass(fullClassName);
  }

  static Object createRecordInstance(Class<?> recordClass, Object[] args) throws Exception {
    RecordComponent[] components = recordClass.getRecordComponents();
    Class<?>[] paramTypes = Arrays.stream(components)
        .map(RecordComponent::getType)
        .toArray(Class<?>[]::new);
    Constructor<?> constructor = recordClass.getDeclaredConstructor(paramTypes);
    return constructor.newInstance(args);
  }

  static void throwCompilationError(DiagnosticCollector<JavaFileObject> diagnostics) {
    StringBuilder errorMsg = new StringBuilder("Compilation failed:\n");
    diagnostics.getDiagnostics().forEach(d -> errorMsg.append(d).append("\n"));
    throw new RuntimeException(errorMsg.toString());
  }

  class InMemorySourceFile extends SimpleJavaFileObject {
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

  class InMemoryClassFile extends SimpleJavaFileObject {
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

  class InMemoryFileManager extends ForwardingJavaFileManager<StandardJavaFileManager> {
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

  class InMemoryClassLoader extends ClassLoader {
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

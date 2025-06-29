// Simple size calculator for primitives only

import org.sample.PrimitiveBenchmark;
import io.github.simbo1905.no.framework.Pickler;

import java.io.*;

class SizeCalc {
  public static void main(String[] args) throws Exception {
    System.out.println("--- Primitive Benchmark Sizes ---");

    final var testData = new PrimitiveBenchmark.AllPrimitives(
        true, (byte) 42, (short) 1000, 'A', 123456, 9876543210L, 3.14f, 2.71828
    );

    // NFP size
    final var primitivePickler = Pickler.forClass(PrimitiveBenchmark.AllPrimitives.class);
    int nfpSize;
    try (final var writeBuffer = primitivePickler.allocateForWriting(1024)) {
      nfpSize = primitivePickler.serialize(writeBuffer, testData);
    }

    // JDK size
    ByteArrayOutputStream jdkOut = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(jdkOut)) {
      oos.writeObject(testData);
    }
    int jdkSize = jdkOut.size();

    System.out.println("NFP: " + nfpSize + " bytes");
    System.out.println("JDK: " + jdkSize + " bytes");
  }
}

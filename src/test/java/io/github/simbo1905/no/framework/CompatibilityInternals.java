// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/// Tests for the internal signature computation used for backwards compatibility
class CompatibilityInternals {

  @Test
  void testSignatureComputation() throws NoSuchAlgorithmException {
    // Test the signature computation algorithm
    String input = "MyThing!ARRAY!LIST!OPTIONAL!Double!compA";

    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));

    // Extract first 8 bytes
    long signature = 0;
    for (int i = 0; i < 8; i++) {
      signature = (signature << 8) | (hash[i] & 0xFF);
    }

    // Verify we get a consistent signature
    assertNotEquals(0, signature);

    // Same input should produce same signature
    byte[] hash2 = digest.digest(input.getBytes(StandardCharsets.UTF_8));
    long signature2 = 0;
    for (int i = 0; i < 8; i++) {
      signature2 = (signature2 << 8) | (hash2[i] & 0xFF);
    }
    assertEquals(signature, signature2);
  }

  @Test
  void testDifferentComponentOrderProducesDifferentSignature() throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");

    // Original order
    String input1 = "Point!int!bool!int!y";
    byte[] hash1 = digest.digest(input1.getBytes(StandardCharsets.UTF_8));
    long sig1 = bytesToLong(hash1);

    // Reordered
    String input2 = "Point!int!y!int!bool";
    byte[] hash2 = digest.digest(input2.getBytes(StandardCharsets.UTF_8));
    long sig2 = bytesToLong(hash2);

    assertNotEquals(sig1, sig2, "Different component order should produce different signatures");
  }

  @Test
  void testAddingComponentChangeSignature() throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");

    // V1
    String input1 = "UserInfo!String!name!int!accessLevel";
    long sig1 = bytesToLong(digest.digest(input1.getBytes(StandardCharsets.UTF_8)));

    // V2 with additional field
    String input2 = "UserInfo!String!name!int!accessLevel!String!department";
    long sig2 = bytesToLong(digest.digest(input2.getBytes(StandardCharsets.UTF_8)));

    assertNotEquals(sig1, sig2, "Adding components should change signature");
  }

  @Test
  void testComplexTypeSignature() throws NoSuchAlgorithmException {
    // Test with nested generics: List<Optional<Double>>[]
    String input = "MyRecord!ARRAY!LIST!OPTIONAL!Double!data";

    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    long signature = bytesToLong(digest.digest(input.getBytes(StandardCharsets.UTF_8)));

    // Different nesting should produce different signature
    String input2 = "MyRecord!LIST!ARRAY!OPTIONAL!Double!data";
    long signature2 = bytesToLong(digest.digest(input2.getBytes(StandardCharsets.UTF_8)));

    assertNotEquals(signature, signature2, "Different type nesting should produce different signatures");
  }

  @Test
  void testMapTypeSignature() throws NoSuchAlgorithmException {
    // Map<String, Integer> should have both key and value types in signature
    String input = "Config!MAP!String!Integer!settings";

    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    long signature = bytesToLong(digest.digest(input.getBytes(StandardCharsets.UTF_8)));

    // Different value type
    String input2 = "Config!MAP!String!Double!settings";
    long signature2 = bytesToLong(digest.digest(input2.getBytes(StandardCharsets.UTF_8)));

    assertNotEquals(signature, signature2, "Different map value types should produce different signatures");
  }

  private long bytesToLong(byte[] hash) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result = (result << 8) | (hash[i] & 0xFF);
    }
    return result;
  }

  @Test
  void testStaticHashClassSignatureMethod() throws Exception {
    // First test the static method directly
    List<String> parts = List.of(
        "Point",      // class simple name
        "INTEGER",    // first component type tag
        "bool",          // first component name
        "INTEGER",    // second component type tag
        "y"           // second component name
    );

    String input = String.join("!", parts);
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
    long expected = bytesToLong(hash);

    // Now call the static method we're about to implement
    record Point(int x, int y) {
    }
    var components = Point.class.getRecordComponents();
    TypeStructure[] types = new TypeStructure[]{
        TypeStructure.analyze(int.class),
        TypeStructure.analyze(int.class)
    };

    long actual = PicklerImpl.hashRecordSignature(Point.class, components, types);
    assertEquals(expected, actual);
  }

  @Test
  void testStaticHashWithComplexTypes() throws Exception {
    // Test with List<Optional<Double>>[]
    List<String> parts = List.of(
        "Complex",
        "ARRAY", "LIST", "OPTIONAL", "DOUBLE", "data"
    );

    String input = String.join("!", parts);
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    long expected = bytesToLong(digest.digest(input.getBytes(StandardCharsets.UTF_8)));

    record Complex(List<Optional<Double>>[] data) {
    }
    var components = Complex.class.getRecordComponents();
    TypeStructure[] types = new TypeStructure[]{
        TypeStructure.analyze(components[0].getGenericType())
    };

    long actual = PicklerImpl.hashRecordSignature(Complex.class, components, types);
    assertEquals(expected, actual);
  }

  // Test records at class level for sealed interface test
  sealed interface TestProtocol permits RecordA, RecordB {
  }

  record RecordA(String name) implements TestProtocol {
  }

  record RecordB(int count, double value) implements TestProtocol {
  }

  @Test
  void testPicklerImplCreatesSignatureArray() throws Exception {
    // Create pickler instance
    Pickler<TestProtocol> pickler = Pickler.forClass(TestProtocol.class);

    // Use reflection to access the typeSignatures field
    var implClass = pickler.getClass();
    var signaturesField = implClass.getDeclaredField("typeSignatures");
    signaturesField.setAccessible(true);
    long[] actualSignatures = (long[]) signaturesField.get(pickler);

    // Should have 2 signatures (RecordA and RecordB)
    assertEquals(2, actualSignatures.length);

    // Compute expected signatures using the static method
    // RecordA
    var componentsA = RecordA.class.getRecordComponents();
    TypeStructure[] typesA = new TypeStructure[]{
        TypeStructure.analyze(String.class)
    };
    long expectedA = PicklerImpl.hashRecordSignature(RecordA.class, componentsA, typesA);

    // RecordB
    var componentsB = RecordB.class.getRecordComponents();
    TypeStructure[] typesB = new TypeStructure[]{
        TypeStructure.analyze(int.class),
        TypeStructure.analyze(double.class)
    };
    long expectedB = PicklerImpl.hashRecordSignature(RecordB.class, componentsB, typesB);

    // They should be in lexicographic order: RecordA before RecordB
    assertEquals(expectedA, actualSignatures[0]);
    assertEquals(expectedB, actualSignatures[1]);
  }
}

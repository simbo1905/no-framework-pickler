// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;
import org.junit.jupiter.api.Assertions;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;


public class PublicApiDemo {

  // @formatter:off
  sealed interface Animal permits Mammal, Bird, Alicorn {}
  sealed interface Mammal extends Animal permits Dog, Cat { }
  sealed interface Bird extends Animal permits Eagle, Penguin {}
  public record Alicorn(String name, String[] magicPowers) implements Animal {
    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      Alicorn alicorn = (Alicorn) o;
      return Objects.equals(name, alicorn.name) && Objects.deepEquals(magicPowers, alicorn.magicPowers);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, Arrays.hashCode(magicPowers));
    }
  }
  public record Dog(String name, int age) implements Mammal {}
  public record Cat(String name, boolean purrs) implements Mammal {}
  public record Eagle(double wingspan) implements Bird {}
  public record Penguin(boolean canSwim) implements Bird {}
  // @formatter:on

  // Create instances of different Animal implementations
  static Dog dog = new Dog("Buddy", 3);
  static Dog dog2 = new Dog("Fido", 2);
  static Animal eagle = new Eagle(2.1);
  static Penguin penguin = new Penguin(true);
  static Alicorn alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

  static List<Animal> animals = List.of(dog, dog2, eagle, penguin, alicorn);

  public static void main(String[] args) {
    // Create a pickler for the sealed interface Animal
    Pickler<Animal> pickler = Pickler.forClass(Animal.class);

    // Rather than just using a fixed buffer if we so wish we can use a fast approximate size of the data
    final int sizeOfMany = animals.size() * Integer.BYTES + animals.stream().mapToInt(pickler::maxSizeOf).sum();

    // Allocate a buffer large enough to hold all serialized animals
    final var buffer = ByteBuffer.allocate(sizeOfMany);

    // We need to know how many animals are in the buffer when we load them back
    buffer.putInt(animals.size());

    // Serialize each animal into the buffer
    for (Animal animal : animals) {
      pickler.serialize(buffer, animal);
    }

    // Prepare the buffer for reading
    buffer.flip();

    // Read the size of the serialized animals
    final int size = buffer.getInt();

    // Deserialize each animal and verify it matches the original
    IntStream.range(0, size).forEach(i -> {
      // Deserialize the next animal from the buffer
      Animal deserializedAnimal = pickler.deserialize(buffer);
      // Get the original animal from the list
      Animal animal = animals.get(i);
      // Verify the deserialized animal matches the original
      Assertions.assertEquals(animal, deserializedAnimal);
    });

    // All animals serialized and deserialized correctly
  }
}


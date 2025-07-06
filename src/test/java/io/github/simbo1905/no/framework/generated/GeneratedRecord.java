package io.github.simbo1905.no.framework.generated;

import io.github.simbo1905.no.framework.TestableRecord;

import java.util.List;

import static io.github.simbo1905.no.framework.ExhaustiveTest.createListArray2D;

public record GeneratedRecord(List<Integer>[][] value) implements TestableRecord {
  public GeneratedRecord() {
    this(null);
  }

  public Object instance() {
    return new GeneratedRecord(createListArray2D(List.of(3)));
  }
}

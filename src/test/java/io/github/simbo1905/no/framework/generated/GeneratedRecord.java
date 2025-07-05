package io.github.simbo1905.no.framework.generated;

import io.github.simbo1905.no.framework.TestableRecord;

import java.util.List;

public record GeneratedRecord(List<Integer[]> value) implements TestableRecord {
  public GeneratedRecord() {
    this(List.of(new Integer[]{1, 2, 3}, new Integer[]{1, 2, 3}));
  }

  public Object instance() {
    return new GeneratedRecord();
  }
}

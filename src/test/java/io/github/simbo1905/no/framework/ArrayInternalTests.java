// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.*;

import static io.github.simbo1905.no.framework.Companion.recordClassHierarchy;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Package-private tests for internal array handling mechanisms
 */
class ArrayInternalTests {

  @BeforeAll
  static void setupLogging() {
    String logLevel = System.getProperty("java.util.logging.ConsoleHandler.level");
    Level level = (logLevel != null) ? Level.parse(logLevel) : Level.WARNING;

    Logger rootLogger = Logger.getLogger("");

    for (Handler handler : rootLogger.getHandlers()) {
      rootLogger.removeHandler(handler);
    }

    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(level);
    consoleHandler.setFormatter(new java.util.logging.Formatter() {
      @Override
      public String format(LogRecord record) {
        return String.format("%-7s %s - %s%n", record.getLevel(), record.getLoggerName(), record.getMessage());
      }
    });

    rootLogger.addHandler(consoleHandler);
    rootLogger.setLevel(level);
  }

  @Test
  void testRecordClassHierarchyDiscoversArrayTypes() {
    // Define a test record with array components
    record TestRecord(String[] strings, Color[] enums, int[] primitives) {
    }

    // Test that recordClassHierarchy discovers array types
    Set<Class<?>> discovered = new HashSet<>(recordClassHierarchy(TestRecord.class));

    // Log what was discovered
    LOGGER.info(() -> "Discovered types: " + discovered.stream()
        .map(Class::getSimpleName)
        .sorted()
        .collect(java.util.stream.Collectors.joining(", ")));

    // Should discover the record itself
    assertTrue(discovered.contains(TestRecord.class), "Should discover TestRecord");

    // Should discover the enum type
    assertTrue(discovered.contains(Color.class), "Should discover Color enum");

    // Should also discover array types for user-defined types
    assertTrue(discovered.contains(String[].class), "Should discover String[] array type");
    assertTrue(discovered.contains(Color[].class), "Should discover Color[] array type");

    // Primitive arrays don't need discovery as they have built-in support
    assertFalse(discovered.contains(int[].class), "Primitive arrays use built-in support");
  }


  enum Color {RED, GREEN, BLUE}
}

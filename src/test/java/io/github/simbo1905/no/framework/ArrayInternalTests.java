// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.BeforeAll;

import java.util.logging.*;

/**
 * Package-private tests for internal array handling mechanisms
 */
public class ArrayInternalTests {

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


  @SuppressWarnings("unused")
  enum Color {RED, GREEN, BLUE}
}

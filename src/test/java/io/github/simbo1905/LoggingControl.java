// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905;

import java.util.logging.*;

/// Modern Java companion object pattern for test logging configuration.
/// Uses sealed interface with config record to avoid singleton anti-patterns.
/// Provides clean, compact output instead of JUL's ugly two-line default format.
public sealed interface LoggingControl permits LoggingControl.Config {

  /// Configuration record for logging setup
  record Config(Level defaultLevel) implements LoggingControl {
  }

  /// Set up clean, compact logging format for tests using functional style.
  /// No instances, no singletons - just clean configuration via records and default methods.
  static void setupCleanLogging(Config config) {
    // Allow CLI override via -Djava.util.logging.ConsoleHandler.level=FINER
    String logLevel = System.getProperty("java.util.logging.ConsoleHandler.level");
    Level level = (logLevel != null) ? Level.parse(logLevel) : config.defaultLevel();

    // Get the root logger to configure globally
    Logger rootLogger = Logger.getLogger("");

    // Remove default handlers to prevent ugly JUL formatting
    for (Handler handler : rootLogger.getHandlers()) {
      rootLogger.removeHandler(handler);
    }

    // Create console handler with clean formatting
    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(level);

    // Custom formatter for compact single-line output (saves tokens and money)
    consoleHandler.setFormatter(new Formatter() {
      @Override
      public String format(LogRecord record) {
        return record.getMessage() + "\n";
      }
    });

    rootLogger.addHandler(consoleHandler);
    rootLogger.setLevel(level);
  }

  /// Convenience method with default WARNING level
  static void setupCleanLogging() {
    Level level = System.getProperty("java.util.logging.ConsoleHandler.level") != null
        ? Level.parse(System.getProperty("java.util.logging.ConsoleHandler.level"))
        : Level.WARNING;
    setupCleanLogging(new Config(level));
  }
}

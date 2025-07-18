// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;

/**
 * Package-private tests for internal array handling mechanisms
 */
public class ArrayInternalTests {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }


  @SuppressWarnings("unused")
  enum Color {RED, GREEN, BLUE}
}

// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package org.sample;

public sealed interface StackCommand permits Push, Pop, Peek {
}

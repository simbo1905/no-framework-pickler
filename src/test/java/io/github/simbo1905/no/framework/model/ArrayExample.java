// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework.model;

import java.util.UUID;

public record ArrayExample(
    String[] stringArray,
    boolean[] booleanArray,
    byte[] byteArray,
    short[] shortArray,
    char[] charArray,
    int[] intArray,
    long[] longArray,
    float[] floatArray,
    double[] doubleArray,
    UUID[] uuidArray,
    Person[] personArray
) {
}

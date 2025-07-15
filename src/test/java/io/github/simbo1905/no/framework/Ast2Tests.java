// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Ast2Tests {

    private static final Logger LOGGER = Logger.getLogger(Ast2Tests.class.getName());

    @BeforeAll
    static void setupLogging() {
        LoggingControl.setupCleanLogging();
    }

    // A trivial record for testing
    public record TrivialRecord(int value) {}

    @Test
    void testTrivialRecordRoundTrip() {
        // 1. Get a pickler for the trivial record
        final Pickler2<TrivialRecord> pickler = Pickler2.forClass(TrivialRecord.class);

        // 2. Create an instance of the record
        final var original = new TrivialRecord(42);
        LOGGER.fine(() -> "Original record: " + original);
        LOGGER.fine(() -> "Original record hashCode: " + original.hashCode());


        // 3. Use maxSizeOf to create a ByteBuffer
        final int maxSize = pickler.maxSizeOf(original);
        LOGGER.fine(() -> "maxSizeOf: " + maxSize);
        final ByteBuffer buffer = ByteBuffer.allocate(maxSize);

        // 4. Write the record to the buffer
        final int bytesWritten = pickler.serialize(buffer, original);
        LOGGER.fine(() -> "Bytes written: " + bytesWritten);


        // 5. Flip the buffer for reading
        buffer.flip();

        // 6. Read the record back
        final TrivialRecord deserialized = pickler.deserialize(buffer);
        LOGGER.fine(() -> "Deserialized record: " + deserialized);
        
        // 7. Assert the deserialized record is equal to the original
        assertEquals(original, deserialized);
    }
}

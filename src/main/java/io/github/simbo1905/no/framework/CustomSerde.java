// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Objects;

final class CustomSerde<T> implements Pickler<T> {
    private final Serde.Sizer sizer;
    private final Serde.Writer writer;
    private final Serde.Reader reader;

    CustomSerde(Serde.Sizer sizer, Serde.Writer writer, Serde.Reader reader) {
        this.sizer = Objects.requireNonNull(sizer);
        this.writer = Objects.requireNonNull(writer);
        this.reader = Objects.requireNonNull(reader);
    }

    @Override
    public int serialize(ByteBuffer buffer, T record) {
        final int start = buffer.position();
        writer.accept(buffer, record);
        return buffer.position() - start;
    }

    @Override
    public T deserialize(ByteBuffer buffer) {
        @SuppressWarnings("unchecked")
        T result = (T) reader.apply(buffer);
        return result;
    }

    @Override
    public int maxSizeOf(T record) {
        return sizer.applyAsInt(record);
    }

    @Override
    public long typeSignature(Class<?> originalClass) {
        throw new UnsupportedOperationException("Custom handlers do not have type signatures.");
    }
}

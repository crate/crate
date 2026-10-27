/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

/**
 * A utility to build XContent (ie json).
 */
public final class XContentBuilder implements Closeable, Flushable {

    /**
     * Create a new {@link XContentBuilder} using the given {@link XContent} content.
     * <p>
     * The builder uses an internal {@link ByteArrayOutputStream} output stream to build the content.
     * </p>
     *
     * @param xContent the {@link XContent}
     * @return a new {@link XContentBuilder}
     * @throws IOException if an {@link IOException} occurs while building the content
     */
    public static XContentBuilder builder(XContent xContent) throws IOException {
        return new XContentBuilder(xContent, new ByteArrayOutputStream());
    }

    private static final Map<Class<?>, Writer> WRITERS;

    static {
        Map<Class<?>, Writer> writers = new HashMap<>();
        writers.put(Boolean.class, (b, v) -> b.value((Boolean) v));
        writers.put(Byte.class, (b, v) -> b.value((Byte) v));
        writers.put(byte[].class, (b, v) -> b.value((byte[]) v));
        writers.put(Double.class, (b, v) -> b.value((Double) v));
        writers.put(double[].class, (b, v) -> b.values((double[]) v));
        writers.put(Float.class, (b, v) -> b.value((Float) v));
        writers.put(float[].class, (b, v) -> b.values((float[]) v));
        writers.put(Integer.class, (b, v) -> b.value((Integer) v));
        writers.put(int[].class, (b, v) -> b.values((int[]) v));
        writers.put(Long.class, (b, v) -> b.value((Long) v));
        writers.put(long[].class, (b, v) -> b.values((long[]) v));
        writers.put(Short.class, (b, v) -> b.value((Short) v));
        writers.put(short[].class, (b, v) -> b.values((short[]) v));
        writers.put(String.class, (b, v) -> b.value((String) v));
        writers.put(String[].class, (b, v) -> b.values((String[]) v));
        writers.put(Locale.class, (b, v) -> b.value(v.toString()));
        writers.put(Class.class, (b, v) -> b.value(v.toString()));
        writers.put(ZonedDateTime.class, (b, v) -> b.value(v.toString()));
        writers.put(BigInteger.class, (b, v) -> b.value((BigInteger) v));
        writers.put(BigDecimal.class, (b, v) -> b.value((BigDecimal) v));
        writers.put(UUID.class, (b, v) -> b.value(v.toString()));

        // Load pluggable extensions
        for (XContentBuilderExtension service : ServiceLoader.load(XContentBuilderExtension.class)) {
            Map<Class<?>, Writer> addlWriters = service.getXContentWriters();
            addlWriters.forEach((key, value) -> Objects.requireNonNull(value,
                "invalid null xcontent writer for class " + key));
            writers.putAll(addlWriters);
        }

        WRITERS = Collections.unmodifiableMap(writers);
    }

    @FunctionalInterface
    public interface Writer {
        void write(XContentBuilder builder, Object value) throws IOException;
    }

    /**
     * XContentGenerator used to build the XContent object
     */
    private final XContentGenerator generator;

    /**
     * Output stream to which the built object is written
     */
    private final OutputStream bos;

    /**
     * Constructs a new builder using the provided XContent and an OutputStream. Make sure
     * to call {@link #close()} when the builder is done with.
     */
    public XContentBuilder(XContent xContent, OutputStream bos) throws IOException {
        this(xContent, bos, null);
    }

    /**
     * Constructs a new builder using the provided XContent and an OutputStream. Make sure
     * to call {@link #close()} when the builder is done with.
     */
    public XContentBuilder(XContent xContent, OutputStream bos, @Nullable String rootValueSeparator) throws IOException {
        this.bos = bos;
        this.generator = xContent.createGenerator(bos, rootValueSeparator);
    }

    public XContentType contentType() {
        return generator.contentType();
    }

    /**
     * @return the output stream to which the built object is being written. Note that is dangerous to modify the stream.
     */
    public OutputStream getOutputStream() {
        return bos;
    }

    public XContentBuilder prettyPrint() {
        generator.usePrettyPrint();
        return this;
    }

    public boolean isPrettyPrint() {
        return generator.isPrettyPrint();
    }

    /**
     * Indicate that the current {@link XContentBuilder} must write a line feed ("\n")
     * at the end of the built object.
     * <p>
     * This only applies for JSON XContent type. It has no effect for other types.
     */
    public XContentBuilder lfAtEnd() {
        generator.usePrintLineFeedAtEnd();
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Structure (object, array, field, null values...)
    //////////////////////////////////

    public XContentBuilder startObject() throws IOException {
        generator.writeStartObject();
        return this;
    }

    public XContentBuilder startObject(String name) throws IOException {
        return field(name).startObject();
    }

    public XContentBuilder endObject() throws IOException {
        generator.writeEndObject();
        return this;
    }

    public XContentBuilder startArray() throws IOException {
        generator.writeStartArray();
        return this;
    }

    public XContentBuilder startArray(String name) throws IOException {
        return field(name).startArray();
    }

    public XContentBuilder endArray() throws IOException {
        generator.writeEndArray();
        return this;
    }

    public XContentBuilder field(String name) throws IOException {
        ensureNameNotNull(name);
        generator.writeFieldName(name);
        return this;
    }

    public XContentBuilder nullField(String name) throws IOException {
        ensureNameNotNull(name);
        generator.writeNullField(name);
        return this;
    }

    public XContentBuilder nullValue() throws IOException {
        generator.writeNull();
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Boolean
    //////////////////////////////////

    public XContentBuilder field(String name, boolean value) throws IOException {
        ensureNameNotNull(name);
        generator.writeBooleanField(name, value);
        return this;
    }

    public XContentBuilder value(Boolean value) throws IOException {
        return (value == null) ? nullValue() : value(value.booleanValue());
    }

    public XContentBuilder value(boolean value) throws IOException {
        generator.writeBoolean(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Byte
    //////////////////////////////////

    public XContentBuilder field(String name, byte value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder value(Byte value) throws IOException {
        return (value == null) ? nullValue() : value(value.byteValue());
    }

    public XContentBuilder value(byte value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Double
    //////////////////////////////////

    private XContentBuilder values(double[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (double b : values) {
            value(b);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Double value) throws IOException {
        return (value == null) ? nullValue() : value(value.doubleValue());
    }

    public XContentBuilder value(double value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Float
    //////////////////////////////////

    public XContentBuilder field(String name, float value) throws IOException {
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    private XContentBuilder values(float[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (float f : values) {
            value(f);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Float value) throws IOException {
        return (value == null) ? nullValue() : value(value.floatValue());
    }

    public XContentBuilder value(float value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Integer
    //////////////////////////////////

    public XContentBuilder field(String name, int value) throws IOException {
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    private XContentBuilder values(int[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (int i : values) {
            value(i);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Integer value) throws IOException {
        return (value == null) ? nullValue() : value(value.intValue());
    }

    public XContentBuilder value(int value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Long
    //////////////////////////////////

    public XContentBuilder field(String name, long value) throws IOException {
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    private XContentBuilder values(long[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (long l : values) {
            value(l);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Long value) throws IOException {
        return (value == null) ? nullValue() : value(value.longValue());
    }

    public XContentBuilder value(long value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Short
    //////////////////////////////////

    private XContentBuilder values(short[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (short s : values) {
            value(s);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Short value) throws IOException {
        return (value == null) ? nullValue() : value(value.shortValue());
    }

    public XContentBuilder value(short value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // BigInteger
    //////////////////////////////////
    public XContentBuilder value(BigInteger value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeNumber(value);
        return this;
    }


    ////////////////////////////////////////////////////////////////////////////
    // BigDecimal
    //////////////////////////////////

    public XContentBuilder value(BigDecimal value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // String
    //////////////////////////////////

    public XContentBuilder field(String name, String value) throws IOException {
        if (value == null) {
            return nullField(name);
        }
        ensureNameNotNull(name);
        generator.writeStringField(name, value);
        return this;
    }

    private XContentBuilder values(String[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (String s : values) {
            value(s);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(String value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeString(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Binary
    //////////////////////////////////

    public XContentBuilder field(String name, byte[] value) throws IOException {
        if (value == null) {
            return nullField(name);
        }
        ensureNameNotNull(name);
        generator.writeBinaryField(name, value);
        return this;
    }

    public XContentBuilder value(byte[] value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeBinary(value);
        return this;
    }

    public XContentBuilder field(String name, byte[] value, int offset, int length) throws IOException {
        return field(name).value(value, offset, length);
    }

    public XContentBuilder value(byte[] value, int offset, int length) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeBinary(value, offset, length);
        return this;
    }

    /**
     * Writes the binary content of the given byte array as UTF-8 bytes.
     *
     * Use {@link XContentParser#charBuffer()} to read the value back
     */
    public XContentBuilder utf8Value(byte[] bytes, int offset, int length) throws IOException {
        generator.writeUTF8String(bytes, offset, length);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Path
    //////////////////////////////////

    public XContentBuilder value(Path value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        return value(value.toString());
    }

    ////////////////////////////////////////////////////////////////////////////
    // Objects
    //
    // These methods are used when the type of value is unknown. It tries to fallback
    // on typed methods and use Object.toString() as a last resort. Always prefer using
    // typed methods over this.
    //////////////////////////////////

    public XContentBuilder field(String name, Object value) throws IOException {
        return field(name).value(value);
    }

    private XContentBuilder values(Object[] values, Map<Class<?>, Writer> writerOverrides) throws IOException {
        if (values == null) {
            return nullValue();
        }
        return value(Arrays.asList(values), writerOverrides);
    }

    public XContentBuilder value(Object value) throws IOException {
        unknownValue(value, Map.of());
        return this;
    }

    private void unknownValue(Object value, Map<Class<?>, Writer> writerOverrides) throws IOException {
        if (value == null) {
            nullValue();
            return;
        }
        Class<? extends Object> clazz = value.getClass();
        Writer writer;
        writer = writerOverrides.get(clazz);
        if (writer == null) {
            writer = WRITERS.get(clazz);
        }
        if (writer != null) {
            writer.write(this, value);
        } else if (value instanceof Path) {
            //Path implements Iterable<Path> and causes endless recursion and a StackOverFlow if treated as an Iterable here
            value((Path) value);
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, ?> valueMap = (Map<String, ?>) value;
            map(valueMap, writerOverrides);
        } else if (value instanceof Iterable) {
            value((Iterable<?>) value, writerOverrides);
        } else if (value instanceof Object[]) {
            values((Object[]) value, writerOverrides);
        } else if (value instanceof Enum<?>) {
            // Write out the Enum toString
            value(Objects.toString(value));
        } else {
            throw new IllegalArgumentException("cannot write xcontent for unknown value of type " + value.getClass());
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Maps & Iterable
    //////////////////////////////////

    public XContentBuilder map(Map<String, ?> values) throws IOException {
        return mapContents(values, true, Map.of());
    }

    public XContentBuilder map(Map<String, ?> values, Map<Class<?>, Writer> writerOverrides) throws IOException {
        return mapContents(values, true, writerOverrides);
    }

    public XContentBuilder mapContents(Map<String, ?> values) throws IOException {
        return mapContents(values, false, Map.of());
    }

    private XContentBuilder mapContents(Map<String, ?> values, boolean writeStartAndEndHeaders, Map<Class<?>, Writer> writerOverrides) throws IOException {
        if (values == null) {
            return nullValue();
        }

        if (writeStartAndEndHeaders) {
            startObject();
        }
        for (Map.Entry<String, ?> value : values.entrySet()) {
            field(value.getKey());
            unknownValue(value.getValue(), writerOverrides);
        }
        if (writeStartAndEndHeaders) {
            endObject();
        }
        return this;
    }

    public XContentBuilder field(String name, Iterable<?> values) throws IOException {
        return field(name).value(values, Map.of());
    }

    private XContentBuilder value(Iterable<?> values, Map<Class<?>, Writer> writerOverrides) throws IOException {
        if (values == null) {
            return nullValue();
        }

        if (values instanceof Path) {
            //treat as single value
            value((Path) values);
        } else {
            startArray();
            for (Object value : values) {
                unknownValue(value, writerOverrides);
            }
            endArray();
        }
        return this;
    }

    @Override
    public void flush() throws IOException {
        generator.flush();
    }

    @Override
    public void close() {
        try {
            generator.close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close the XContentBuilder", e);
        }
    }

    public XContentGenerator generator() {
        return this.generator;
    }

    static void ensureNameNotNull(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Field name cannot be null");
        }
    }
}

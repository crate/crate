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

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

public interface XContentGenerator extends Closeable, Flushable {

    XContentType contentType();

    void usePrettyPrint();

    boolean isPrettyPrint();

    void usePrintLineFeedAtEnd();

    void writeStartObject() throws IOException;

    void writeEndObject() throws IOException;

    void writeStartArray() throws IOException;

    void writeEndArray() throws IOException;

    void writeFieldName(String name) throws IOException;

    void writeNull() throws IOException;

    void writeNullField(String name) throws IOException;

    void writeBooleanField(String name, boolean value) throws IOException;

    void writeBoolean(boolean value) throws IOException;

    void writeNumberField(String name, double value) throws IOException;

    void writeNumber(double value) throws IOException;

    void writeNumberField(String name, float value) throws IOException;

    void writeNumber(float value) throws IOException;

    void writeNumberField(String name, int value) throws IOException;

    void writeNumber(int value) throws IOException;

    void writeNumberField(String name, long value) throws IOException;

    void writeNumber(long value) throws IOException;

    void writeNumber(short value) throws IOException;

    void writeNumber(BigInteger value) throws IOException;

    void writeNumberField(String name, BigInteger value) throws IOException;

    void writeNumber(BigDecimal value) throws IOException;

    void writeNumberField(String name, BigDecimal value) throws IOException;

    void writeStringField(String name, String value) throws IOException;

    void writeString(String value) throws IOException;

    void writeString(char[] text, int offset, int len) throws IOException;

    void writeUTF8String(byte[] value, int offset, int length) throws IOException;

    void writeBinaryField(String name, byte[] value) throws IOException;

    void writeBinary(byte[] value) throws IOException;

    void writeBinary(byte[] value, int offset, int length) throws IOException;

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     * @deprecated use {@link #writeRawField(String, InputStream, XContentType)} to avoid content type auto-detection
     */
    @Deprecated
    void writeRawField(String name, InputStream value) throws IOException;

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     */
    void writeRawField(String name, InputStream value, XContentType xContentType) throws IOException;

    /**
     * Writes a raw value taken from the bytes in the stream
     */
    void writeRawValue(InputStream value, XContentType xContentType) throws IOException;

    void copyCurrentStructure(XContentParser parser) throws IOException;

    default void copyCurrentEvent(XContentParser parser) throws IOException {
        switch (parser.currentToken()) {
            case START_OBJECT:
                writeStartObject();
                break;
            case END_OBJECT:
                writeEndObject();
                break;
            case START_ARRAY:
                writeStartArray();
                break;
            case END_ARRAY:
                writeEndArray();
                break;
            case FIELD_NAME:
                writeFieldName(parser.currentName());
                break;
            case VALUE_STRING:
                if (parser.hasTextCharacters()) {
                    writeString(parser.textCharacters(), parser.textOffset(), parser.textLength());
                } else {
                    writeString(parser.text());
                }
                break;
            case VALUE_NUMBER:
                switch (parser.numberType()) {
                    case INT:
                        writeNumber(parser.intValue());
                        break;
                    case LONG:
                        writeNumber(parser.longValue());
                        break;
                    case FLOAT:
                        writeNumber(parser.floatValue());
                        break;
                    case DOUBLE:
                        writeNumber(parser.doubleValue());
                        break;
                }
                break;
            case VALUE_BOOLEAN:
                writeBoolean(parser.booleanValue());
                break;
            case VALUE_NULL:
                writeNull();
                break;
            case VALUE_EMBEDDED_OBJECT:
                writeBinary(parser.binaryValue());
        }
    }

    /**
     * Returns {@code true} if this XContentGenerator has been closed. A closed generator can not do any more output.
     */
    boolean isClosed();

    void configure(JsonGenerator.Feature f, boolean state);

    boolean isEnabled(JsonGenerator.Feature f);
}

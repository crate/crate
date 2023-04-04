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

package org.elasticsearch.common.xcontent.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.base.GeneratorBase;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.core.json.JsonWriteContext;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

public class JsonXContentGenerator implements XContentGenerator {

    /** Generator used to write content **/
    protected final JsonGenerator generator;

    /**
     * Reference to base generator because
     * writing raw values needs a specific method call.
     */
    private final GeneratorBase base;

    private final OutputStream os;

    private boolean writeLineFeedAtEnd;
    private static final SerializedString LF = new SerializedString("\n");
    private static final DefaultPrettyPrinter.Indenter INDENTER = new DefaultIndenter("  ", LF.getValue());
    private boolean prettyPrint = false;

    public JsonXContentGenerator(JsonGenerator jsonGenerator, OutputStream os) {
        this.os = os;
        if (jsonGenerator instanceof GeneratorBase) {
            this.base = (GeneratorBase) jsonGenerator;
        } else {
            this.base = null;
        }
        JsonGenerator generator = jsonGenerator;
        this.generator = generator;
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public final void usePrettyPrint() {
        generator.setPrettyPrinter(new DefaultPrettyPrinter().withObjectIndenter(INDENTER).withArrayIndenter(INDENTER));
        prettyPrint = true;
    }

    @Override
    public boolean isPrettyPrint() {
        return this.prettyPrint;
    }

    @Override
    public void usePrintLineFeedAtEnd() {
        writeLineFeedAtEnd = true;
    }

    private JsonGenerator getLowLevelGenerator() {
        return generator;
    }

    @Override
    public void writeStartObject() throws IOException {
        generator.writeStartObject();
    }

    @Override
    public void writeEndObject() throws IOException {
        generator.writeEndObject();
    }


    @Override
    public void writeStartArray() throws IOException {
        generator.writeStartArray();
    }

    @Override
    public void writeEndArray() throws IOException {
        generator.writeEndArray();
    }

    @Override
    public void writeFieldName(String name) throws IOException {
        generator.writeFieldName(name);
    }

    @Override
    public void writeNull() throws IOException {
        generator.writeNull();
    }

    @Override
    public void writeNullField(String name) throws IOException {
        generator.writeNullField(name);
    }

    @Override
    public void writeBooleanField(String name, boolean value) throws IOException {
        generator.writeBooleanField(name, value);
    }

    @Override
    public void writeBoolean(boolean value) throws IOException {
        generator.writeBoolean(value);
    }

    @Override
    public void writeNumberField(String name, double value) throws IOException {
        generator.writeNumberField(name, value);
    }

    @Override
    public void writeNumber(double value) throws IOException {
        generator.writeNumber(value);
    }

    @Override
    public void writeNumberField(String name, float value) throws IOException {
        generator.writeNumberField(name, value);
    }

    @Override
    public void writeNumber(float value) throws IOException {
        generator.writeNumber(value);
    }

    @Override
    public void writeNumberField(String name, int value) throws IOException {
        generator.writeNumberField(name, value);
    }

    @Override
    public void writeNumberField(String name, BigInteger value) throws IOException {
        // as jackson's JsonGenerator doesn't have this method for BigInteger
        // we have to implement it ourselves
        generator.writeFieldName(name);
        generator.writeNumber(value);
    }

    @Override
    public void writeNumberField(String name, BigDecimal value) throws IOException {
        generator.writeNumberField(name, value);
    }

    @Override
    public void writeNumber(int value) throws IOException {
        generator.writeNumber(value);
    }

    @Override
    public void writeNumberField(String name, long value) throws IOException {
        generator.writeNumberField(name, value);
    }

    @Override
    public void writeNumber(long value) throws IOException {
        generator.writeNumber(value);
    }

    @Override
    public void writeNumber(short value) throws IOException {
        generator.writeNumber(value);
    }

    @Override
    public void writeNumber(BigInteger value) throws IOException {
        generator.writeNumber(value);
    }

    @Override
    public void writeNumber(BigDecimal value) throws IOException {
        generator.writeNumber(value);
    }

    @Override
    public void writeStringField(String name, String value) throws IOException {
        generator.writeStringField(name, value);
    }

    @Override
    public void writeString(String value) throws IOException {
        generator.writeString(value);
    }

    @Override
    public void writeString(char[] value, int offset, int len) throws IOException {
        generator.writeString(value, offset, len);
    }

    @Override
    public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
        generator.writeUTF8String(value, offset, length);
    }

    @Override
    public void writeBinaryField(String name, byte[] value) throws IOException {
        generator.writeBinaryField(name, value);
    }

    @Override
    public void writeBinary(byte[] value) throws IOException {
        generator.writeBinary(value);
    }

    @Override
    public void writeBinary(byte[] value, int offset, int len) throws IOException {
        generator.writeBinary(value, offset, len);
    }

    public void writeEndRaw() {
        assert base != null : "JsonGenerator should be of instance GeneratorBase but was: " + generator.getClass();
        if (base != null) {
            JsonStreamContext context = base.getOutputContext();
            assert (context instanceof JsonWriteContext) : "Expected an instance of JsonWriteContext but was: " + context.getClass();
            ((JsonWriteContext) context).writeValue();
        }
    }

    @Override
    public void writeRawValue(InputStream stream, XContentType xContentType) throws IOException {
        if (mayWriteRawData(xContentType) == false) {
            copyRawValue(stream, xContentType.xContent());
        } else {
            if (generator.getOutputContext().getCurrentName() != null) {
                // If we've just started a field we'll need to add the separator
                generator.writeRaw(':');
            }
            flush();
            stream.transferTo(os);
            writeEndRaw();
        }
    }

    private boolean mayWriteRawData(XContentType contentType) {
        // When the content is in a different format than the current generator,
        // we need to copy the whole structure so that it will be correctly
        // converted
        return supportsRawWrites()
                && contentType == contentType()
                && prettyPrint == false;
    }

    /** Whether this generator supports writing raw data directly */
    protected boolean supportsRawWrites() {
        return true;
    }

    protected void copyRawValue(InputStream stream, XContent xContent) throws IOException {
        // EMPTY is safe here because we never call namedObject
        try (XContentParser parser = xContent
                 // It's okay to pass the throwing deprecation handler because we
                 // should not be writing raw fields when generating JSON
                 .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream)) {
            copyCurrentStructure(parser);
        }
    }

    @Override
    public void copyCurrentStructure(XContentParser parser) throws IOException {
        // the start of the parser
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        if (parser instanceof JsonXContentParser) {
            generator.copyCurrentStructure(((JsonXContentParser) parser).parser);
        } else {
            copyCurrentStructure(this, parser);
        }
    }

    /**
     * Low level implementation detail of {@link XContentGenerator#copyCurrentStructure(XContentParser)}.
     */
    private static void copyCurrentStructure(XContentGenerator destination, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();

        // Let's handle field-name separately first
        if (token == XContentParser.Token.FIELD_NAME) {
            destination.writeFieldName(parser.currentName());
            token = parser.nextToken();
            // fall-through to copy the associated value
        }

        switch (token) {
            case START_ARRAY:
                destination.writeStartArray();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    copyCurrentStructure(destination, parser);
                }
                destination.writeEndArray();
                break;
            case START_OBJECT:
                destination.writeStartObject();
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    copyCurrentStructure(destination, parser);
                }
                destination.writeEndObject();
                break;
            default: // others are simple:
                destination.copyCurrentEvent(parser);
        }
    }

    @Override
    public void flush() throws IOException {
        generator.flush();
    }

    @Override
    public void close() throws IOException {
        if (generator.isClosed()) {
            return;
        }
        JsonStreamContext context = generator.getOutputContext();
        if ((context != null) && (context.inRoot() == false)) {
            throw new IOException("Unclosed object or array found");
        }
        if (writeLineFeedAtEnd) {
            flush();
            // Bypass generator to always write the line feed
            getLowLevelGenerator().writeRaw(LF);
        }
        generator.close();
    }

    @Override
    public boolean isClosed() {
        return generator.isClosed();
    }

    @Override
    public void configure(JsonGenerator.Feature f, boolean state) {
        generator.configure(f, state);
    }

    @Override
    public boolean isEnabled(JsonGenerator.Feature f) {
        return generator.isEnabled(f);
    }
}

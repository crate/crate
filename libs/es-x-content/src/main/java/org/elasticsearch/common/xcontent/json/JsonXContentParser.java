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
import java.nio.CharBuffer;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.AbstractXContentParser;

import io.crate.common.io.IOUtils;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.TokenStreamLocation;

public class JsonXContentParser extends AbstractXContentParser {

    final JsonParser parser;

    public JsonXContentParser(NamedXContentRegistry xContentRegistry,
                              DeprecationHandler deprecationHandler,
                              JsonParser parser) {
        super(xContentRegistry, deprecationHandler);
        this.parser = parser;
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public Token nextToken() throws IOException {
        return convertToken(parser.nextToken());
    }

    @Override
    public void skipChildren() throws IOException {
        parser.skipChildren();
    }

    @Override
    public Token currentToken() {
        return convertToken(parser.currentToken());
    }

    @Override
    public NumberType numberType() throws IOException {
        return convertNumberType(parser.getNumberType());
    }

    @Override
    public String currentName() throws IOException {
        return parser.currentName();
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        return parser.getBooleanValue();
    }

    @Override
    public String text() throws IOException {
        if (currentToken().isValue()) {
            return parser.getString();
        }
        throw new IllegalStateException("Can't get text on a " + currentToken() + " at " + getTokenLocation());
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        return CharBuffer.wrap(parser.getStringCharacters(), parser.getStringOffset(), parser.getStringLength());
    }

    @Override
    public Object objectText() throws IOException {
        JsonToken currentToken = parser.currentToken();
        if (currentToken == JsonToken.VALUE_STRING) {
            return text();
        } else if (currentToken == JsonToken.VALUE_NUMBER_INT || currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
            return parser.getNumberValue();
        } else if (currentToken == JsonToken.VALUE_TRUE) {
            return Boolean.TRUE;
        } else if (currentToken == JsonToken.VALUE_FALSE) {
            return Boolean.FALSE;
        } else if (currentToken == JsonToken.VALUE_NULL) {
            return null;
        } else {
            return text();
        }
    }

    @Override
    public boolean hasStringCharacters() {
        return parser.hasStringCharacters();
    }

    @Override
    public char[] stringCharacters() throws IOException {
        return parser.getStringCharacters();
    }

    @Override
    public int textLength() throws IOException {
        return parser.getStringLength();
    }

    @Override
    public int textOffset() throws IOException {
        return parser.getStringOffset();
    }

    @Override
    public Number numberValue() throws IOException {
        return parser.getNumberValue();
    }

    @Override
    public short doShortValue() throws IOException {
        return parser.getShortValue();
    }

    @Override
    public int doIntValue() throws IOException {
        return parser.getIntValue();
    }

    @Override
    public long doLongValue() throws IOException {
        return parser.getLongValue();
    }

    @Override
    public float doFloatValue() throws IOException {
        return parser.getFloatValue();
    }

    @Override
    public double doDoubleValue() throws IOException {
        return parser.getDoubleValue();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return parser.getBinaryValue();
    }

    @Override
    public XContentLocation getTokenLocation() {
        TokenStreamLocation loc = parser.currentTokenLocation();
        if (loc == null) {
            return null;
        }
        return new XContentLocation(loc.getLineNr(), loc.getColumnNr());
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(parser);
    }

    private NumberType convertNumberType(JsonParser.NumberType numberType) {
        switch (numberType) {
            case INT:
                return NumberType.INT;
            case LONG:
                return NumberType.LONG;
            case FLOAT:
                return NumberType.FLOAT;
            case DOUBLE:
                return NumberType.DOUBLE;
            default:
                throw new IllegalStateException("No matching token for number_type [" + numberType + "]");
        }
    }

    private Token convertToken(JsonToken token) {
        if (token == null) {
            return null;
        }
        switch (token) {
            case PROPERTY_NAME:
                return Token.FIELD_NAME;
            case VALUE_FALSE:
            case VALUE_TRUE:
                return Token.VALUE_BOOLEAN;
            case VALUE_STRING:
                return Token.VALUE_STRING;
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                return Token.VALUE_NUMBER;
            case VALUE_NULL:
                return Token.VALUE_NULL;
            case START_OBJECT:
                return Token.START_OBJECT;
            case END_OBJECT:
                return Token.END_OBJECT;
            case START_ARRAY:
                return Token.START_ARRAY;
            case END_ARRAY:
                return Token.END_ARRAY;
            case VALUE_EMBEDDED_OBJECT:
                return Token.VALUE_EMBEDDED_OBJECT;
            default:
                throw new IllegalStateException("No matching token for json_token [" + token + "]");
        }
    }

    @Override
    public boolean isClosed() {
        return parser.isClosed();
    }
}

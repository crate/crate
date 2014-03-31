/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.symbol;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.lucene.util.BytesRef;
import io.crate.DataType;
import io.crate.TimestampFormat;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

public class StringLiteral extends Literal<BytesRef, StringLiteral> {

    private static final Map<String, Boolean> booleanMap = ImmutableMap.<String, Boolean>builder()
            .put("f", false)
            .put("false", false)
            .put("t", true)
            .put("true", true)
            .build();

    public static final SymbolFactory<StringLiteral> FACTORY = new SymbolFactory<StringLiteral>() {
        @Override
        public StringLiteral newInstance() {
            return new StringLiteral();
        }
    };
    private BytesRef value;

    public StringLiteral(String value) {
        this(new BytesRef(value));
    }

    public StringLiteral(BytesRef value) {
        assert value != null;
        this.value = value;
    }

    StringLiteral() {
    }

    @Override
    public String valueAsString() {
        return value().utf8ToString();
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.STRING_LITERAL;
    }

    @Override
    public BytesRef value() {
        return value;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitStringLiteral(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = in.readBytesRef();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesRef(value);
    }

    @Override
    public DataType valueType() {
        return DataType.STRING;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StringLiteral that = (StringLiteral) o;

        if (!value.equals(that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public int compareTo(StringLiteral o) {
        return Ordering.natural().compare(value, o.value);
    }

    public Object convertValueTo(DataType type, String value) {
        switch (type) {
            case LONG:
                return new Long(value);
            case TIMESTAMP:
                try {
                    return new Long(value);
                } catch (NumberFormatException e) {
                    return TimestampFormat.parseTimestampString(value);
                }
            case INTEGER:
                return new Integer(value);
            case DOUBLE:
                return new Double(value);
            case FLOAT:
                return new Float(value);
            case SHORT:
                return new Short(value);
            case BYTE:
                return new Byte(value);
            case IP:
            case STRING:
                return value;
            case BOOLEAN:
                Boolean convertedValue = booleanMap.get(value.toLowerCase());
                if (convertedValue == null) {
                    return super.convertTo(type);
                }
                return convertedValue;
            default:
                return super.convertValueTo(type, new BytesRef(value));
        }
    }

    @Override
    public Object convertValueTo(DataType type, BytesRef value) {
        if (valueType() == type) {
            return value;
        }
        return convertValueTo(type, value.utf8ToString());
    }

    @Override
    public Literal convertTo(DataType type) {
        if (valueType() == type) {
            return this;
        }
        return Literal.forType(type, convertValueTo(type));
    }
}

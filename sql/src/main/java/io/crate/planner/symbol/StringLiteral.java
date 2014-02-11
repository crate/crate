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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StringLiteral extends Literal<String, StringLiteral> {

    private static final FormatDateTimeFormatter dateTimeFormatter = Joda.forPattern("dateOptionalTime", Locale.ROOT);
    private static final TimeUnit timeUnit = TimeUnit.MILLISECONDS;
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
    private String value;

    public StringLiteral(String value) {
        Preconditions.checkNotNull(value);
        this.value = value;
    }

    StringLiteral() {}

    @Override
    public SymbolType symbolType() {
        return SymbolType.STRING_LITERAL;
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitStringLiteral(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(value);
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

    @Override
    public Literal convertTo(DataType type) {
        Object convertedValue;
        switch (type) {
            case LONG:
                convertedValue = new Long(value());
                break;
            case TIMESTAMP:
                convertedValue = parseTimestampString();
                break;
            case INTEGER:
                convertedValue = new Integer(value());
                break;
            case DOUBLE:
                convertedValue = new Double(value());
                break;
            case FLOAT:
                convertedValue = new Float(value());
                break;
            case SHORT:
                convertedValue = new Short(value());
                break;
            case BYTE:
                convertedValue = new Byte(value());
                break;
            case BOOLEAN:
                convertedValue = booleanMap.get(value().toLowerCase());
                if (convertedValue == null) {
                    super.convertTo(type);
                }
                break;
            default:
                return super.convertTo(type);
        }
        return Literal.forType(type, convertedValue);
    }

    private long parseTimestampString() {
        try {
            return dateTimeFormatter.parser().parseMillis(value);
        } catch (RuntimeException e) {
            try {
                long time = Long.parseLong(value);
                return timeUnit.toMillis(time);
            } catch (NumberFormatException e1) {
                throw new UnsupportedOperationException("failed to parse timestamp field [" + value + "], tried both date format [" + dateTimeFormatter.format() + "], and timestamp number with locale [" + dateTimeFormatter.locale() + "]", e);
            }
        }
    }
}

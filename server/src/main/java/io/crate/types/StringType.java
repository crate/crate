/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.types;

import io.crate.Streamer;
import io.crate.common.unit.TimeValue;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class StringType extends DataType<String> implements Streamer<String> {

    public static final int ID = 4;
    public static final StringType INSTANCE = new StringType();
    public static final String T = "t";
    public static final String F = "f";

    private final int lengthLimit;

    public static StringType of(List<Integer> parameters) {
        if (parameters.size() != 1) {
            throw new IllegalArgumentException(
                "The number of parameters for the text data is wrong: " + parameters.size());
        }
        return StringType.of(parameters.get(0));
    }

    public static StringType of(int lengthLimit) {
        if (lengthLimit < 0) {
            throw new IllegalArgumentException(
                "Invalid text data type length limit: " + lengthLimit);
        }
        return new StringType(lengthLimit);
    }

    private StringType(int lengthLimit) {
        this.lengthLimit = lengthLimit;
    }

    protected StringType() {
        this(Integer.MAX_VALUE);
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.STRING;
    }

    @Override
    public String getName() {
        return "text";
    }

    public int lengthLimit() {
        return lengthLimit;
    }

    public boolean unbound() {
        return lengthLimit == Integer.MAX_VALUE;
    }

    @Override
    public TypeSignature getTypeSignature() {
        if (unbound()) {
            return super.getTypeSignature();
        } else {
            return new TypeSignature(
                getName(),
                List.of(TypeSignature.of(lengthLimit)));
        }
    }

    @Override
    public List<DataType<?>> getTypeParameters() {
        if (unbound()) {
            return List.of();
        } else {
            return List.of(DataTypes.INTEGER);
        }
    }

    @Override
    public Streamer<String> streamer() {
        return this;
    }

    @Override
    public String value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof BytesRef) {
            return ((BytesRef) value).utf8ToString();
        }
        if (value instanceof Boolean) {
            if ((boolean) value) {
                return T;
            } else {
                return F;
            }
        }
        if (value instanceof Map) {
            try {
                //noinspection unchecked
                return Strings.toString(XContentFactory.jsonBuilder().map((Map<String, ?>) value));
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot cast `" + value + "` to type TEXT", e);
            }
        }
        if (value instanceof Collection) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Cannot cast %s to type TEXT", value));
        }
        if (value.getClass().isArray()) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Cannot cast %s to type TEXT", Arrays.toString((Object[]) value)));
        }
        if (value instanceof TimeValue) {
            return ((TimeValue) value).getStringRep();
        }
        return value.toString();
    }

    @Override
    public int compare(String val1, String val2) {
        return val1.compareTo(val2);
    }

    @Override
    public String readValueFrom(StreamInput in) throws IOException {
        return in.readOptionalString();
    }

    @Override
    public void writeValueTo(StreamOutput out, String v) throws IOException {
        out.writeOptionalString(v);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        StringType that = (StringType) o;
        return lengthLimit == that.lengthLimit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), lengthLimit);
    }
}

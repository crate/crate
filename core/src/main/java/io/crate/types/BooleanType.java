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

import com.google.common.collect.ImmutableMap;
import io.crate.Streamer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

public class BooleanType extends DataType<Boolean> implements DataTypeFactory, Streamer<Boolean>, FixedWidthType {

    public static final int ID = 3;
    public static final BooleanType INSTANCE = new BooleanType();

    private BooleanType() {}

    private static final Map<BytesRef, Boolean> BOOLEAN_MAP = ImmutableMap.<BytesRef, Boolean>builder()
            .put(BytesRefs2.LOWER_SHORT_FALSE, Boolean.FALSE)
            .put(BytesRefs2.UPPER_SHORT_FALSE, Boolean.FALSE)
            .put(BytesRefs2.LOWER_SHORT_TRUE, Boolean.TRUE)
            .put(BytesRefs2.UPPER_SHORT_TRUE, Boolean.TRUE)
            .put(BytesRefs2.LOWER_FALSE, Boolean.FALSE)
            .put(BytesRefs2.UPPER_FALSE, Boolean.FALSE)
            .put(BytesRefs2.LOWER_TRUE, Boolean.TRUE)
            .put(BytesRefs2.UPPER_TRUE, Boolean.TRUE)
            .build();


    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "boolean";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    public Boolean value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof BytesRef) {
            return booleanFromBytesRef(((BytesRef) value));
        }
        if (value instanceof Number) {
            return booleanFromNumber((Number)value);
        }
        if (value instanceof String) {
            return booleanFromString((String) value);
        }
        throw illegalConversion(value);
    }

    private Boolean booleanFromBytesRef(BytesRef value) {
        Boolean bool = BOOLEAN_MAP.get(value);
        if (bool == null) {
            throw illegalConversion(value);
        }
        return bool;
    }

    private ClassCastException illegalConversion(Object value) {
        return new ClassCastException("Can't cast '" + value + "' to boolean");
    }

    private Boolean booleanFromString(String value) {
        switch (value.length()) {
            case 1:
                if ("t".equalsIgnoreCase(value)) {
                    return Boolean.TRUE;
                } else if ("f".equalsIgnoreCase(value)) {
                    return Boolean.FALSE;
                }
                break;
            case 4:
                if ("true".equalsIgnoreCase(value)) {
                    return Boolean.TRUE;
                }
                break;
            case 5:
                if ("false".equalsIgnoreCase(value)) {
                    return Boolean.FALSE;
                }
                break;
        }
        throw illegalConversion(value);
    }

    private Boolean booleanFromNumber(Number value) {
        if(value.doubleValue() > 0.0) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    @Override
    public int compareValueTo(Boolean val1, Boolean val2) {
        return Boolean.compare(val1, val2);
    }

    @Override
    public DataType<?> create() {
        return INSTANCE;
    }

    @Override
    public Boolean readValueFrom(StreamInput in) throws IOException {
        return in.readOptionalBoolean();
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        out.writeOptionalBoolean((Boolean)v);
    }

    @Override
    public int fixedSize() {
        return 8;
    }
}

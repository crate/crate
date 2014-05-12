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
import java.util.Locale;
import java.util.Map;

public class BooleanType extends DataType<Boolean> implements DataTypeFactory, Streamer<Boolean> {

    public static final int ID = 3;
    public static final BooleanType INSTANCE = new BooleanType();

    private BooleanType() {}

    private static final Map<String, Boolean> booleanMap = ImmutableMap.<String, Boolean>builder()
            .put("f", false)
            .put("false", false)
            .put("t", true)
            .put("true", true)
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
        if (value instanceof String) {
            return booleanFromString((String) value);
        }
        if (value instanceof BytesRef) {
            return booleanFromString(((BytesRef) value).utf8ToString());
        }
        return (Boolean)value;
    }

    private Boolean booleanFromString(String value) {
        String lowerValue = value.toLowerCase(Locale.ENGLISH);
        Boolean boolValue = booleanMap.get(lowerValue);
        if (boolValue == null) {
            throw new IllegalArgumentException("Can't convert \"" + value + "\" to boolean");
        } else {
            return boolValue;
        }
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
}

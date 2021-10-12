/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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


package io.crate.types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;

import io.crate.Streamer;

public final class JsonType extends DataType<String> implements Streamer<String> {

    public static final int ID = 26;
    public static final JsonType INSTANCE = new JsonType();

    @Override
    public int compare(String o1, String o2) {
        return o1.compareTo(o2);
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.UNCHECKED_OBJECT;
    }

    @Override
    public String getName() {
        return "json";
    }

    @Override
    public Streamer<String> streamer() {
        return this;
    }

    @Override
    public String sanitizeValue(Object value) {
        return (String) value;
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
    public String implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return (String) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public String explicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value instanceof Map<?, ?> map) {
            try {
                return Strings.toString(XContentFactory.jsonBuilder().map((Map<String, ?>) map));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return (String) value;
    }
}

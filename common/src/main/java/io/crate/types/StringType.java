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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Comparator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;

import io.crate.Streamer;

public class StringType extends DataType<String> implements Streamer<String> {

    public static final int ID = 4;
    public static final StringType INSTANCE = new StringType();
    public static final String T = "t";
    public static final String F = "f";

    protected StringType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.StringType;
    }

    @Override
    public String getName() {
        return "text";
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
                return Strings.toString(XContentFactory.jsonBuilder().map((Map) value));
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
    public int compareValueTo(String val1, String val2) {
        return Comparator.<String>nullsFirst(Comparator.naturalOrder()).compare(val1, val2);
    }

    @Override
    public String readValueFrom(StreamInput in) throws IOException {
        return in.readOptionalString();
    }

    @Override
    public void writeValueTo(StreamOutput out, String v) throws IOException {
        out.writeOptionalString(v);
    }
}

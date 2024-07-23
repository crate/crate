/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.dml;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import io.crate.sql.tree.BitString;

/**
 * Converts the translog as represented by a Map to a BytesReference
 */
public class TranslogWriter {

    private TranslogWriter() { }

    public static BytesReference convert(Map<String, Object> content) {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (
                XContentBuilder builder = XContentFactory.json(output)) {
                builder.startObject();
                for (var entry : content.entrySet()) {
                    writeValue(entry.getKey(), entry.getValue(), builder);
                }
                builder.endObject();
            }
            // XContentBuilder must be closed before we access the bytes
            return output.bytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeValue(String key, Object value, XContentBuilder builder) throws IOException {
        builder.field(key);
        switch (value) {
            case null -> writeValue(value, builder);
            case List<?> l -> {
                builder.startArray();
                for (var o : l) {
                    writeValue(o, builder);
                }
                builder.endArray();
            }
            case Map<?, ?> m -> {
                builder.startObject();
                for (var entry : m.entrySet()) {
                    writeValue(entry.getKey().toString(), entry.getValue(), builder);
                }
                builder.endObject();
            }
            case Object _ -> writeValue(value, builder);
        }
    }

    private static void writeValue(Object value, XContentBuilder builder) throws IOException {
        switch (value) {
            case null -> builder.nullValue();
            case BitString bitString -> builder.value(bitString.bitSet().toByteArray());
            case Object o -> builder.value(o);
        }
    }
}

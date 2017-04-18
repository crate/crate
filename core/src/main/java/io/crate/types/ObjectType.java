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
import io.crate.core.collections.MapComparator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

public class ObjectType extends DataType<Map<String, Object>>
    implements Streamer<Map<String, Object>>, DataTypeFactory {

    public static final ObjectType INSTANCE = new ObjectType();
    public static final int ID = 12;

    private ObjectType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "object";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> value(Object value) {
        if (value instanceof BytesRef) {
            return mapFromBytesRef((BytesRef) value);
        }
        return (Map<String, Object>) value;
    }

    private static Map<String, Object> mapFromBytesRef(BytesRef value) {
        try {
            // It is safe to use NamedXContentRegistry.EMPTY here because this never uses namedObject
            XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                value.bytes, value.offset, value.length);
            return parser.map();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compareValueTo(Map<String, Object> val1, Map<String, Object> val2) {
        return MapComparator.compareMaps(val1, val2);
    }

    @Override
    public DataType<?> create() {
        return INSTANCE;
    }

    // TODO: require type info from each child and then use typed streamer for contents of the map
    // ?

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> readValueFrom(StreamInput in) throws IOException {
        return (Map<String, Object>) in.readGenericValue();
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        out.writeGenericValue(v);
    }
}

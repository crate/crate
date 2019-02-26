/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.es.test;

import io.crate.es.ElasticsearchParseException;
import io.crate.es.cluster.AbstractNamedDiffable;
import io.crate.es.cluster.NamedDiff;
import io.crate.es.cluster.metadata.MetaData;
import io.crate.es.common.io.stream.StreamInput;
import io.crate.es.common.io.stream.StreamOutput;
import io.crate.es.common.xcontent.XContentBuilder;
import io.crate.es.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.Function;

public abstract class TestCustomMetaData extends AbstractNamedDiffable<MetaData.Custom> implements MetaData.Custom {
    private final String data;

    protected TestCustomMetaData(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestCustomMetaData that = (TestCustomMetaData) o;

        if (!data.equals(that.data)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    protected static <T extends TestCustomMetaData> T readFrom(Function<String, T> supplier, StreamInput in) throws IOException {
        return supplier.apply(in.readString());
    }

    public static NamedDiff<MetaData.Custom> readDiffFrom(String name, StreamInput in)  throws IOException {
        return readDiffFrom(MetaData.Custom.class, name, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getData());
    }

    @SuppressWarnings("unchecked")
    public static <T extends MetaData.Custom> T fromXContent(Function<String, MetaData.Custom> supplier, XContentParser parser)
        throws IOException {
        XContentParser.Token token;
        String data = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                if ("data".equals(currentFieldName)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                        throw new ElasticsearchParseException("failed to parse snapshottable metadata, invalid data type");
                    }
                    data = parser.text();
                } else {
                    throw new ElasticsearchParseException("failed to parse snapshottable metadata, unknown field [{}]", currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("failed to parse snapshottable metadata");
            }
        }
        if (data == null) {
            throw new ElasticsearchParseException("failed to parse snapshottable metadata, data not found");
        }
        return (T) supplier.apply(data);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("data", getData());
        return builder;
    }

    @Override
    public String toString() {
        return "[" + getWriteableName() + "][" + data +"]";
    }
}

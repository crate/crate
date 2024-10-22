/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;

public class AliasMetadata extends AbstractDiffable<AliasMetadata> {

    private final String alias;

    public AliasMetadata(String alias) {
        this.alias = alias;
    }

    public String alias() {
        return alias;
    }

    public String getAlias() {
        return alias();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AliasMetadata that = (AliasMetadata) o;

        return Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return alias != null ? alias.hashCode() : 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(alias());
        if (out.getVersion().before(Version.V_5_3_0)) {
            out.writeBoolean(false); // no filter
            out.writeBoolean(false); // no index routing
            out.writeBoolean(false); // no search routing
            out.writeOptionalBoolean(null); // writeIndex
        }
    }

    public AliasMetadata(StreamInput in) throws IOException {
        alias = in.readString();
        if (in.getVersion().before(Version.V_5_3_0)) {
            if (in.readBoolean()) {
                CompressedXContent.readCompressedString(in); // filter
            }
            if (in.readBoolean()) {
                in.readString(); // indexRouting
            }
            if (in.readBoolean()) {
                in.readString(); // searchRouting
            }
            in.readOptionalBoolean(); // writeIndex
        }
    }

    public static Diff<AliasMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(AliasMetadata::new, in);
    }

    public static class Builder {

        private final String alias;

        public Builder(String alias) {
            this.alias = alias;
        }

        public Builder(AliasMetadata aliasMetadata) {
            this(aliasMetadata.alias());
        }

        public String alias() {
            return alias;
        }

        public AliasMetadata build() {
            return new AliasMetadata(alias);
        }

        public static AliasMetadata fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder(parser.currentName());

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                // no data...
                return builder.build();
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("filter".equals(currentFieldName)) {
                        parser.mapOrdered();
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                }
            }
            return builder.build();
        }
    }
}

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
import java.util.Map;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentType;

import io.crate.Constants;
import io.crate.server.xcontent.XContentHelper;

/**
 * Mapping configuration for a type.
 */
public class MappingMetadata extends AbstractDiffable<MappingMetadata> {

    private final CompressedXContent source;

    public MappingMetadata(CompressedXContent mapping) throws IOException {
        this.source = mapping;
    }

    public MappingMetadata(Map<String, Object> mapping) throws IOException {
        this.source = new CompressedXContent(
            (builder, params) -> builder.mapContents(mapping), XContentType.JSON, ToXContent.EMPTY_PARAMS);
    }

    public CompressedXContent source() {
        return this.source;
    }

    /**
     * Converts the serialized compressed form of the mappings into a parsed map.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> sourceAsMap() throws ElasticsearchParseException {
        Map<String, Object> mapping = XContentHelper.convertToMap(source.compressedReference(), true, XContentType.JSON).map();
        if (mapping.size() == 1 && mapping.containsKey(Constants.DEFAULT_MAPPING_TYPE)) {
            // the type name is the root value, reduce it
            mapping = (Map<String, Object>) mapping.get(Constants.DEFAULT_MAPPING_TYPE);
        }
        return mapping;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_5_2_0)) {
            out.writeString(Constants.DEFAULT_MAPPING_TYPE);
        }
        source().writeTo(out);
        if (out.getVersion().before(Version.V_5_0_0)) {
            // routing
            out.writeBoolean(false);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetadata that = (MappingMetadata) o;

        return source.equals(that.source);
    }

    @Override
    public int hashCode() {
        return source.hashCode();
    }

    public MappingMetadata(StreamInput in) throws IOException {
        if (in.getVersion().before(Version.V_5_2_0)) {
            in.readString(); // type, was always "default"
        }
        source = CompressedXContent.readCompressedString(in);
        if (in.getVersion().before(Version.V_5_0_0)) {
            // routing
            in.readBoolean();
        }
    }

    public static Diff<MappingMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(MappingMetadata::new, in);
    }
}

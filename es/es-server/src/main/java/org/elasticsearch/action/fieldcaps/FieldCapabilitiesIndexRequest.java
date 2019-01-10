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

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class FieldCapabilitiesIndexRequest
    extends SingleShardRequest<FieldCapabilitiesIndexRequest> {

    private String[] fields;
    private OriginalIndices originalIndices;

    // For serialization
    FieldCapabilitiesIndexRequest() {}

    FieldCapabilitiesIndexRequest(String[] fields, String index, OriginalIndices originalIndices) {
        super(index);
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("specified fields can't be null or empty");
        }
        this.fields = fields;
        assert index != null;
        this.index(index);
        this.originalIndices = originalIndices;
    }

    public String[] fields() {
        return fields;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        fields = in.readStringArray();
        if (in.getVersion().onOrAfter(Version.V_6_2_0)) {
            originalIndices = OriginalIndices.readOriginalIndices(in);
        } else {
            originalIndices = OriginalIndices.NONE;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(fields);
        if (out.getVersion().onOrAfter(Version.V_6_2_0)) {
            OriginalIndices.writeOriginalIndices(originalIndices, out);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}

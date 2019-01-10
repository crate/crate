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

package org.elasticsearch.action.admin.indices.cache.clear;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ClearIndicesCacheRequest extends BroadcastRequest<ClearIndicesCacheRequest> {

    private boolean queryCache = false;
    private boolean fieldDataCache = false;
    private boolean requestCache = false;
    private String[] fields = Strings.EMPTY_ARRAY;


    public ClearIndicesCacheRequest() {
    }

    public ClearIndicesCacheRequest(String... indices) {
        super(indices);
    }

    public boolean queryCache() {
        return queryCache;
    }

    public ClearIndicesCacheRequest queryCache(boolean queryCache) {
        this.queryCache = queryCache;
        return this;
    }

    public boolean requestCache() {
        return this.requestCache;
    }

    public ClearIndicesCacheRequest requestCache(boolean requestCache) {
        this.requestCache = requestCache;
        return this;
    }

    public boolean fieldDataCache() {
        return this.fieldDataCache;
    }

    public ClearIndicesCacheRequest fieldDataCache(boolean fieldDataCache) {
        this.fieldDataCache = fieldDataCache;
        return this;
    }

    public ClearIndicesCacheRequest fields(String... fields) {
        this.fields = fields == null ? Strings.EMPTY_ARRAY : fields;
        return this;
    }

    public String[] fields() {
        return this.fields;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        queryCache = in.readBoolean();
        fieldDataCache = in.readBoolean();
        if (in.getVersion().before(Version.V_6_0_0_beta1)) {
            in.readBoolean(); // recycler
        }
        fields = in.readStringArray();
        requestCache = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(queryCache);
        out.writeBoolean(fieldDataCache);
        if (out.getVersion().before(Version.V_6_0_0_beta1)) {
            out.writeBoolean(false); // recycler
        }
        out.writeStringArrayNullable(fields);
        out.writeBoolean(requestCache);
    }
}

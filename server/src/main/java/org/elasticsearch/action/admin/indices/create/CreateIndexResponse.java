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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * A response for a create index action.
 */
public class CreateIndexResponse extends ShardsAcknowledgedResponse {

    private final String index;

    protected CreateIndexResponse(boolean acknowledged, boolean shardsAcknowledged, String index) {
        super(acknowledged, shardsAcknowledged);
        this.index = index;
    }

    public CreateIndexResponse(StreamInput in) throws IOException {
        super(in);
        index = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
    }

    /**
     * Returns true if the requisite number of shards were started before
     * returning from the index creation operation. If {@link #isAcknowledged()}
     * is false, then this also returns false.
     *
     * @deprecated use {@link #isShardsAcknowledged()}
     */
    @Deprecated
    public boolean isShardsAcked() {
        return isShardsAcknowledged();
    }

    public String index() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            CreateIndexResponse that = (CreateIndexResponse) o;
            return Objects.equals(index, that.index);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), index);
    }
}

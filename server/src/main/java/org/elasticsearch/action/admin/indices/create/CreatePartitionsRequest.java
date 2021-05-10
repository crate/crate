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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class CreatePartitionsRequest extends AcknowledgedRequest<CreatePartitionsRequest> {

    private final Collection<String> indices;
    private final UUID jobId;

    /**
     * Constructs a new request to create indices with the specified names.
     */
    public CreatePartitionsRequest(Collection<String> indices, UUID jobId) {
        this.indices = indices;
        this.jobId = jobId;
    }

    public Collection<String> indices() {
        return indices;
    }

    public UUID jobId() {
        return jobId;
    }

    public CreatePartitionsRequest(StreamInput in) throws IOException {
        super(in);
        jobId = new UUID(in.readLong(), in.readLong());
        int numIndices = in.readVInt();
        List<String> indicesList = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            indicesList.add(in.readString());
        }
        this.indices = indicesList;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
        out.writeVInt(indices.size());
        for (String index : indices) {
            out.writeString(index);
        }
    }

}

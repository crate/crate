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

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CreatePartitionsRequest extends AcknowledgedRequest<CreatePartitionsRequest> {

    private final Collection<String> indices;

    /**
     * Constructs a new request to create indices with the specified names.
     */
    public CreatePartitionsRequest(Collection<String> indices) {
        this.indices = indices;
    }

    public Collection<String> indices() {
        return indices;
    }

    public CreatePartitionsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_5_3_0)) {
            // The only usage of jobId was removed in https://github.com/crate/crate/commit/31e0f7f447eaa006e756c20bd32346b2680ebee6
            // Nodes < 5.3.0 still send UUID which is written as 2 longs, we consume them but don't create an UUID out of them.
            in.readLong();
            in.readLong();
        }
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
        if (out.getVersion().before(Version.V_5_3_0)) {
            // Nodes < 5.3.0 still expect 2 longs.
            // They are used to construct an UUID but last time they were actually used in CrateDB 0.55.0.
            // Hence, sending dummy values.
            out.writeLong(0L);
            out.writeLong(0L);
        }

        out.writeVInt(indices.size());
        for (String index : indices) {
            out.writeString(index);
        }
    }

}

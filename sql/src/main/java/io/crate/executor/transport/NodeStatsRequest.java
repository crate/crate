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

package io.crate.executor.transport;

import io.crate.metadata.ReferenceIdent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NodeStatsRequest extends TransportRequest {

    private String nodeId;
    private List<ReferenceIdent> referenceIdents = new ArrayList<>();

    public NodeStatsRequest() {
    }

    public NodeStatsRequest(String nodeId, List<ReferenceIdent> referenceIdents) {
        this.nodeId = nodeId;
        this.referenceIdents = referenceIdents;
    }

    public String nodeId() {
        return nodeId;
    }

    public List<ReferenceIdent> referenceIdents() {
        return referenceIdents;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        nodeId = in.readString();
        int refIndentsSize = in.readVInt();
        for (int i = 0; i < refIndentsSize; i++) {
            ReferenceIdent referenceIdent = new ReferenceIdent();
            referenceIdent.readFrom(in);
            referenceIdents.add(referenceIdent);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        out.writeVInt(referenceIdents.size());
        for (ReferenceIdent referenceIdent : referenceIdents) {
            referenceIdent.writeTo(out);
        }
    }
}

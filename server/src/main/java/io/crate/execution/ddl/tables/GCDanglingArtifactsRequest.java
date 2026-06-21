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

package io.crate.execution.ddl.tables;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jspecify.annotations.Nullable;

public class GCDanglingArtifactsRequest extends AcknowledgedRequest<GCDanglingArtifactsRequest> {

    public static final GCDanglingArtifactsRequest ALL = GCDanglingArtifactsRequest.ofIndexUUIDs(List.of());

    private final List<String> indexUUIDs;
    @Nullable
    private final String resizeSourceUUID;

    /// @param indexUUIDs indexUUIDs to delete. If empty, all dangling indices UUIDs are deleted.
    public static GCDanglingArtifactsRequest ofIndexUUIDs(List<String> indexUUIDs) {
        return new GCDanglingArtifactsRequest(indexUUIDs, null);
    }

    // Resolve resize artifacts on the master node to avoid relying on a handler node's local cluster state.
    public static GCDanglingArtifactsRequest forResizeArtifactsOf(String sourceIndexUUID) {
        return new GCDanglingArtifactsRequest(List.of(), sourceIndexUUID);
    }

    private GCDanglingArtifactsRequest(List<String> indexUUIDs, @Nullable String resizeSourceUUID) {
        super();
        this.indexUUIDs = indexUUIDs;
        this.resizeSourceUUID = resizeSourceUUID;
    }

    public GCDanglingArtifactsRequest(StreamInput in) throws IOException {
        super(in);
        Version version = in.getVersion();
        if (version.onOrAfter(Version.V_6_1_1)) {
            this.indexUUIDs = in.readStringList();
        } else {
            this.indexUUIDs = List.of();
        }
        if (version.onOrAfter(Version.V_6_4_0)) {
            this.resizeSourceUUID = in.readOptionalString();
        } else {
            this.resizeSourceUUID = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Version version = out.getVersion();
        if (version.onOrAfter(Version.V_6_1_1)) {
            out.writeStringCollection(indexUUIDs);
        }
        if (version.onOrAfter(Version.V_6_4_0)) {
            out.writeOptionalString(resizeSourceUUID);
        }
    }

    public List<String> indexUUIDs() {
        return indexUUIDs;
    }

    @Nullable
    public String resizeSourceUUID() {
        return resizeSourceUUID;
    }
}

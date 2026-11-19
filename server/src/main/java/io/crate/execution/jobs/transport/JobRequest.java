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

package io.crate.execution.jobs.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.support.NodeRequest;
import io.crate.metadata.settings.SessionSettings;

public class JobRequest extends TransportRequest {

    public static NodeRequest<JobRequest> of(String nodeId,
                                             UUID jobId,
                                             SessionSettings sessionSettings,
                                             String coordinatorNodeId,
                                             Collection<? extends NodeOperation> nodeOperations,
                                             boolean enableProfiling,
                                             Version minNodeVersion) {
        return new NodeRequest<>(
            nodeId,
            new JobRequest(jobId, sessionSettings, coordinatorNodeId, nodeOperations, enableProfiling, minNodeVersion)
        );
    }

    private final UUID jobId;
    private final SessionSettings sessionSettings;
    private final String coordinatorNodeId;
    private final Collection<? extends NodeOperation> nodeOperations;
    private final boolean enableProfiling;
    private final Version senderVersion;
    private final Version minNodeVersion;

    private JobRequest(UUID jobId,
                       SessionSettings sessionSettings,
                       String coordinatorNodeId,
                       Collection<? extends NodeOperation> nodeOperations,
                       boolean enableProfiling,
                       Version minNodeVersion) {
        this.jobId = jobId;
        this.coordinatorNodeId = coordinatorNodeId;
        this.sessionSettings = sessionSettings;
        this.nodeOperations = nodeOperations;
        this.enableProfiling = enableProfiling;
        this.senderVersion = Version.CURRENT;
        this.minNodeVersion = minNodeVersion;
    }

    public UUID jobId() {
        return jobId;
    }

    public Collection<? extends NodeOperation> nodeOperations() {
        return nodeOperations;
    }

    public String coordinatorNodeId() {
        return coordinatorNodeId;
    }

    public boolean enableProfiling() {
        return enableProfiling;
    }

    public SessionSettings sessionSettings() {
        return sessionSettings;
    }

    public Version senderVersion() {
        return senderVersion;
    }

    public Version minNodeVersion() {
        return minNodeVersion;
    }

    JobRequest(StreamInput in) throws IOException {
        super(in);
        senderVersion = in.getVersion();

        jobId = new UUID(in.readLong(), in.readLong());
        coordinatorNodeId = in.readString();

        int numNodeOperations = in.readVInt();
        ArrayList<NodeOperation> nodeOperations = new ArrayList<>(numNodeOperations);
        for (int i = 0; i < numNodeOperations; i++) {
            nodeOperations.add(new NodeOperation(in));
        }
        this.nodeOperations = nodeOperations;
        enableProfiling = in.readBoolean();

        sessionSettings = new SessionSettings(in);
        if (hasMinNodeVersion(in.getVersion())) {
            minNodeVersion = Version.readVersion(in);
        } else {
            minNodeVersion = in.getVersion();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
        out.writeString(coordinatorNodeId);

        out.writeVInt(nodeOperations.size());
        for (NodeOperation nodeOperation : nodeOperations) {
            nodeOperation.writeTo(out);
        }

        out.writeBoolean(enableProfiling);

        sessionSettings.writeTo(out);
        if (hasMinNodeVersion(out.getVersion())) {
            Version.writeVersion(minNodeVersion, out);
        }
    }

    private static boolean hasMinNodeVersion(Version version) {
        return version.after(Version.V_6_2_11) && version.before(Version.fromString("6.3.0"));
    }
}


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

package io.crate.execution.jobs.kill;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;
import org.jspecify.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;

public class KillJobsNodeRequest extends TransportRequest {

    private final List<String> excludedNodeIds;
    private final KillJobsRequest killJobsRequest;

    public KillJobsNodeRequest(List<String> excludedNodeIds, Collection<UUID> jobsToKill, String userName, @Nullable String reason) {
        this.killJobsRequest = new KillJobsRequest(jobsToKill, userName, reason);
        this.excludedNodeIds = excludedNodeIds;
    }

    public List<String> excludedNodeIds() {
        return excludedNodeIds;
    }

    public KillJobsRequest innerRequest() {
        return killJobsRequest;
    }

    @VisibleForTesting
    public static class KillJobsRequest extends TransportRequest {

        private final Collection<UUID> toKill;
        private final String userName;

        @Nullable
        private final String reason;


        /**
         * @param userName user that invoked the kill. If the kill is system generated use User.CRATE_USER.name()
         */
        private KillJobsRequest(Collection<UUID> jobsToKill, String userName, @Nullable String reason) {
            this.toKill = jobsToKill;
            this.userName = userName;
            this.reason = reason;
        }

        public Collection<UUID> toKill() {
            return toKill;
        }

        KillJobsRequest(StreamInput in) throws IOException {
            super(in);
            int numJobs = in.readVInt();
            toKill = new ArrayList<>(numJobs);
            for (int i = 0; i < numJobs; i++) {
                UUID job = new UUID(in.readLong(), in.readLong());
                toKill.add(job);
            }
            reason = in.readOptionalString();
            userName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            int numJobs = toKill.size();
            out.writeVInt(numJobs);
            for (UUID job : toKill) {
                out.writeLong(job.getMostSignificantBits());
                out.writeLong(job.getLeastSignificantBits());
            }
            out.writeOptionalString(reason);
            out.writeString(userName);
        }

        @Override
        public String toString() {
            return "KillJobsRequest{" + toKill + '}';
        }

        @Nullable
        public String reason() {
            return reason;
        }

        public String userName() {
            return userName;
        }
    }
}

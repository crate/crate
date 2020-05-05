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

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import javax.annotation.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * Contains information about restores snapshot
 */
public class RestoreSnapshotResponse extends TransportResponse implements ToXContentObject {

    @Nullable
    private final RestoreInfo restoreInfo;

    RestoreSnapshotResponse(@Nullable RestoreInfo restoreInfo) {
        this.restoreInfo = restoreInfo;
    }

    public RestoreSnapshotResponse(StreamInput in) throws IOException {
        restoreInfo = in.readOptionalWriteable(RestoreInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(restoreInfo);
    }

    public RestStatus status() {
        if (restoreInfo == null) {
            return RestStatus.ACCEPTED;
        }
        return restoreInfo.status();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (restoreInfo != null) {
            builder.field("snapshot");
            restoreInfo.toXContent(builder, params);
        } else {
            builder.field("accepted", true);
        }
        builder.endObject();
        return builder;
    }

    public static final ConstructingObjectParser<RestoreSnapshotResponse, Void> PARSER = new ConstructingObjectParser<>(
        "restore_snapshot",
        true,
        v -> {
            RestoreInfo restoreInfo = (RestoreInfo) v[0];
            Boolean accepted = (Boolean) v[1];
            assert (accepted == null && restoreInfo != null) || (accepted != null && accepted && restoreInfo == null) :
                "accepted: [" + accepted + "], restoreInfo: [" + restoreInfo + "]";
            return new RestoreSnapshotResponse(restoreInfo);
        }
    );

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestoreSnapshotResponse that = (RestoreSnapshotResponse) o;
        return Objects.equals(restoreInfo, that.restoreInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(restoreInfo);
    }

    @Override
    public String toString() {
        return "RestoreSnapshotResponse{" + "restoreInfo=" + restoreInfo + '}';
    }
}

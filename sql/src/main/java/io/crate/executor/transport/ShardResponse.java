/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.base.MoreObjects;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShardResponse extends ReplicationResponse implements WriteResponse {

    /**
     * Represents a failure.
     */
    public static class Failure implements Streamable {

        private String id;
        private String message;
        private boolean versionConflict;

        Failure() {
        }

        Failure(String id, String message, boolean versionConflict) {
            this.id = id;
            this.message = message;
            this.versionConflict = versionConflict;
        }

        public String id() {
            return id;
        }

        public String message() {
            return this.message;
        }

        public boolean versionConflict() {
            return versionConflict;
        }

        static Failure readFailure(StreamInput in) throws IOException {
            Failure failure = new Failure();
            failure.readFrom(in);
            return failure;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            id = in.readString();
            message = in.readString();
            versionConflict = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeString(message);
            out.writeBoolean(versionConflict);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("message", message)
                .add("versionConflict", versionConflict)
                .toString();
        }
    }

    private IntArrayList locations = new IntArrayList();
    private List<Failure> failures = new ArrayList<>();
    @Nullable
    private Exception failure;
    private boolean forcedRefresh;

    public ShardResponse() {
    }

    public void add(int location) {
        locations.add(location);
        failures.add(null);
    }

    public void add(int location, Failure failure) {
        locations.add(location);
        failures.add(failure);
    }

    public IntArrayList itemIndices() {
        return locations;
    }

    public List<Failure> failures() {
        return failures;
    }

    public void failure(@Nullable Exception failure) {
        this.failure = failure;
    }

    public Exception failure() {
        return failure;
    }

    @Override
    public void setForcedRefresh(boolean forcedRefresh) {
        this.forcedRefresh = forcedRefresh;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        locations = new IntArrayList(size);
        failures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            if (in.readBoolean()) {
                failures.add(Failure.readFailure(in));
            } else {
                failures.add(null);
            }
        }
        if (in.readBoolean()) {
            failure = in.readException();
        }
        forcedRefresh = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(locations.size());
        for (int i = 0; i < locations.size(); i++) {
            out.writeVInt(locations.get(i));
            if (failures.get(i) == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                failures.get(i).writeTo(out);
            }
        }
        if (failure != null) {
            out.writeBoolean(true);
            out.writeException(failure);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(forcedRefresh);
    }

}

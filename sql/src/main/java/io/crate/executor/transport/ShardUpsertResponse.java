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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkProcessorResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShardUpsertResponse extends ActionResponse implements BulkProcessorResponse<ShardUpsertResponse.Response> {

    /**
     * Represents a failure.
     */
    public static class Failure implements Streamable {

        private String id;
        private String message;
        private boolean versionConflict;

        Failure() {
        }
         
        public Failure(String id, String message, boolean versionConflict) {
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

        public static Failure readFailure(StreamInput in) throws IOException {
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
    }
     
    public static class Response implements Streamable {
         
        private String id;
        private long version;
        private boolean created;
         
        Response() {
        }
         
        public Response(String id, long version, boolean created) {
            this.id = id;
            this.version = version;
            this.created = created;
        }
         
        public String id() {
            return this.id;
        }
         
        /**
         * Returns the current version of the doc indexed.
         */
        public long version() {
            return this.version;
        }
         
        /**
         * Returns true if document was created due to an UPSERT operation
         */
        public boolean created() {
            return this.created;
        }
         
        public static Response readResponse(StreamInput in) throws IOException {
            Response response = new Response();
            response.readFrom(in);
            return response;
        }
         
        @Override
        public void readFrom(StreamInput in) throws IOException {
            id = in.readString();
            version = in.readLong();
            created = in.readBoolean();
        }
         
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeLong(version);
            out.writeBoolean(created);
        }
    }



    private String index;
    private IntArrayList locations = new IntArrayList();
    private List<Response> responses = new ArrayList<>();
    private List<Failure> failures = new ArrayList<>();

    public ShardUpsertResponse() {
    }

    public ShardUpsertResponse(String index) {
        this.index = index;
    }

    public String index() {
        return this.index;
    }

    public void add(int location, Response response) {
        locations.add(location);
        responses.add(response);
        failures.add(null);
    }

    public void add(int location, Failure failure) {
        locations.add(location);
        responses.add(null);
        failures.add(failure);
    }

    @Override
    public IntArrayList itemIndices() {
        return locations;
    }

    @Override
    public List<Response> responses() {
        return responses;
    }

    public List<Failure> failures() {
        return failures;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readSharedString();
        int size = in.readVInt();
        locations = new IntArrayList(size);
        responses = new ArrayList<>(size);
        failures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            if (in.readBoolean()) {
                responses.add(Response.readResponse(in));
            } else {
                responses.add(null);
            }
            if (in.readBoolean()) {
                failures.add(Failure.readFailure(in));
            } else {
                failures.add(null);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeSharedString(index);
        out.writeVInt(locations.size());
        for (int i = 0; i < locations.size(); i++) {
            out.writeVInt(locations.get(i));
            if (responses.get(i) == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                responses.get(i).writeTo(out);
            }
            if (failures.get(i) == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                failures.get(i).writeTo(out);
            }
        }
    }

}

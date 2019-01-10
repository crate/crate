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

package org.elasticsearch.action.termvectors;

import com.carrotsearch.hppc.IntArrayList;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiTermVectorsShardResponse extends ActionResponse {

    IntArrayList locations;
    List<TermVectorsResponse> responses;
    List<MultiTermVectorsResponse.Failure> failures;

    MultiTermVectorsShardResponse() {
        locations = new IntArrayList();
        responses = new ArrayList<>();
        failures = new ArrayList<>();
    }

    public void add(int location, TermVectorsResponse response) {
        locations.add(location);
        responses.add(response);
        failures.add(null);
    }

    public void add(int location, MultiTermVectorsResponse.Failure failure) {
        locations.add(location);
        responses.add(null);
        failures.add(failure);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        locations = new IntArrayList(size);
        responses = new ArrayList<>(size);
        failures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            if (in.readBoolean()) {
                TermVectorsResponse response = new TermVectorsResponse();
                response.readFrom(in);
                responses.add(response);
            } else {
                responses.add(null);
            }
            if (in.readBoolean()) {
                failures.add(MultiTermVectorsResponse.Failure.readFailure(in));
            } else {
                failures.add(null);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
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
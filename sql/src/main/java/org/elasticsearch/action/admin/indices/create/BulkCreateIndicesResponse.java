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

package org.elasticsearch.action.admin.indices.create;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BulkCreateIndicesResponse extends AcknowledgedResponse {

    private List<CreateIndexResponse> responses = Lists.newArrayList();
    private Set<String> alreadyExisted = Sets.newHashSet();

    private static final Predicate<? super CreateIndexResponse> IS_ACKNOWLEDGED = new Predicate<CreateIndexResponse>() {
        @Override
        public boolean apply(CreateIndexResponse input) {
            return input.isAcknowledged();
        }
    };

    BulkCreateIndicesResponse() {

    }

    BulkCreateIndicesResponse(List<CreateIndexResponse> responses) {
        super(Iterables.all(responses, IS_ACKNOWLEDGED));
        this.responses = responses;
        this.alreadyExisted = Sets.newHashSet();
    }

    public List<CreateIndexResponse> responses() {
        return responses;
    }

    public Set<String> alreadyExisted() {
        return alreadyExisted;
    }

    public void addAlreadyExisted(String alreadyExisted) {
        this.alreadyExisted.add(alreadyExisted);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int numResponses = in.readVInt();
        responses = new ArrayList<>(numResponses);
        for (int i = 0; i < numResponses; i++) {
            CreateIndexResponse response = new CreateIndexResponse();
            response.readFrom(in);
            responses.add(response);
        }
        int numAlreadyexisted = in.readVInt();
        alreadyExisted = new HashSet<>(numAlreadyexisted);
        for (int i = 0; i < numAlreadyexisted; i++) {
            alreadyExisted.add(in.readString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(responses.size());
        for (CreateIndexResponse response : responses) {
            response.writeTo(out);
        }
        out.writeVInt(alreadyExisted.size());
        for (String alreadyThere : alreadyExisted) {
            out.writeString(alreadyThere);
        }
    }
}

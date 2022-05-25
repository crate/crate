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

package io.crate.statistics;

import io.crate.metadata.SimpleReference;
import io.crate.metadata.RelationName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class FetchSampleRequest extends TransportRequest {

    private final RelationName relationName;
    private final List<SimpleReference> columns;
    private final int maxSamples;

    public FetchSampleRequest(RelationName relationName, List<SimpleReference> columns, int maxSamples) {
        this.relationName = relationName;
        this.columns = columns;
        this.maxSamples = maxSamples;
    }

    public FetchSampleRequest(StreamInput in) throws IOException {
        this.relationName = new RelationName(in);
        this.maxSamples = in.readVInt();
        int numColumns = in.readVInt();
        this.columns = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            columns.add(SimpleReference.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        relationName.writeTo(out);
        out.writeVInt(maxSamples);
        out.writeVInt(columns.size());
        for (SimpleReference column : columns) {
            SimpleReference.toStream(column, out);
        }
    }

    public RelationName relation() {
        return relationName;
    }

    public List<SimpleReference> columns() {
        return columns;
    }

    public int maxSamples() {
        return maxSamples;
    }
}

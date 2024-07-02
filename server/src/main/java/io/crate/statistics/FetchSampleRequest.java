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

import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class FetchSampleRequest extends TransportRequest {

    private final RelationName relationName;
    private final List<Reference> columns;

    public FetchSampleRequest(RelationName relationName, List<Reference> columns, Version nodeVersion) {
        this.relationName = relationName;
        this.columns = columns;
        if (nodeVersion.before(Version.V_5_7_0)) {
            throw new UnsupportedOperationException("Cannot run ANALYZE request in a mixed cluster with nodes older than 5.7.0");
        }
    }

    public FetchSampleRequest(StreamInput in) throws IOException {
        if (in.getVersion().before(Version.V_5_7_0)) {
            throw new UnsupportedOperationException("Cannot run ANALYZE request in a mixed cluster with nodes older than 5.7.0");
        }
        this.relationName = new RelationName(in);
        if (in.getVersion().before(Version.V_5_7_0)) {
            in.readVInt();  // Old max samples value
        }
        int numColumns = in.readVInt();
        this.columns = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            columns.add(Reference.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        relationName.writeTo(out);
        if (out.getVersion().before(Version.V_5_7_0)) {
            out.writeVInt(30_000);  // Old max samples value
        }
        out.writeVInt(columns.size());
        for (Reference column : columns) {
            Reference.toStream(out, column);
        }
    }

    public RelationName relation() {
        return relationName;
    }

    public List<Reference> columns() {
        return columns;
    }

    @Override
    public int hashCode() {
        return Objects.hash(relationName, columns);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof FetchSampleRequest other)) {
            return false;
        }
        return relationName.equals(other.relation()) && columns.equals(other.columns());
    }
}

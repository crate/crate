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

package io.crate.execution.engine.collect.stats;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import io.crate.common.unit.TimeValue;
import io.crate.execution.support.NodeRequest;
import io.crate.metadata.ColumnIdent;

public class NodeStatsRequest extends NodeRequest<NodeStatsRequest.StatsRequest> {

    private final TimeValue timeout;

    public NodeStatsRequest(String nodeId, TimeValue timeout, Set<ColumnIdent> columns) {
        super(nodeId, new StatsRequest(columns));
        this.timeout = timeout;
    }

    public TimeValue timeout() {
        return timeout;
    }

    static class StatsRequest extends TransportRequest {

        private final Set<ColumnIdent> columns;

        private StatsRequest(Set<ColumnIdent> columns) {
            this.columns = columns;
        }

        public Set<ColumnIdent> columnIdents() {
            return columns;
        }

        StatsRequest(StreamInput in) throws IOException {
            super(in);
            columns = new HashSet<>();
            int columnIdentsSize = in.readVInt();
            for (int i = 0; i < columnIdentsSize; i++) {
                columns.add(ColumnIdent.of(in));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(columns.size());
            for (ColumnIdent columnIdent : columns) {
                columnIdent.writeTo(out);
            }
        }
    }
}

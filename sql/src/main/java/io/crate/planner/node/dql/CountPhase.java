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

package io.crate.planner.node.dql;

import com.google.common.collect.Sets;
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhaseVisitor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

public class CountPhase implements ExecutionPhase {

    public static final ExecutionPhaseFactory<CountPhase> FACTORY = new ExecutionPhaseFactory<CountPhase>() {
        @Override
        public CountPhase create() {
            return new CountPhase();
        }
    };
    private UUID jobId;
    private int executionPhaseId;
    private Routing routing;
    private WhereClause whereClause;

    CountPhase() {}

    public CountPhase(UUID jobId, int executionPhaseId, Routing routing, WhereClause whereClause) {
        this.jobId = jobId;
        this.executionPhaseId = executionPhaseId;
        this.routing = routing;
        this.whereClause = whereClause;
    }

    @Override
    public Type type() {
        return Type.COUNT;
    }

    @Override
    public String name() {
        return "count";
    }

    @Override
    public UUID jobId() {
        return jobId;
    }

    public Routing routing() {
        return routing;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    @Override
    public int executionPhaseId() {
        return executionPhaseId;
    }

    @Override
    public Set<String> executionNodes() {
        if (routing.isNullRouting()) {
            return routing.nodes();
        } else {
            return Sets.filter(routing.nodes(), TableInfo.IS_NOT_NULL_NODE_ID);
        }
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitCountPhase(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        jobId = new UUID(in.readLong(), in.readLong());
        executionPhaseId = in.readVInt();
        routing = new Routing();
        routing.readFrom(in);
        whereClause = new WhereClause(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert jobId != null : "jobId must not be null";
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
        out.writeVInt(executionPhaseId);
        routing.writeTo(out);
        whereClause.writeTo(out);
    }
}

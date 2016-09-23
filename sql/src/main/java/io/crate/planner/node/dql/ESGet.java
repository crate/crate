/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node.dql;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.where.DocKeys;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.PlanAndPlannedAnalyzedRelation;
import io.crate.planner.PlanVisitor;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;

import java.util.List;
import java.util.UUID;


public class ESGet extends PlanAndPlannedAnalyzedRelation {

    private final static boolean[] EMPTY_REVERSE_FLAGS = new boolean[0];
    private final static Boolean[] EMPTY_NULLS_FIRST = new Boolean[0];
    private final DocTableInfo tableInfo;
    private final QuerySpec querySpec;
    private final List<Symbol> sortSymbols;
    private final boolean[] reverseFlags;
    private final Boolean[] nullsFirst;
    private final int executionPhaseId;
    private final UUID jobId;
    private final DocKeys docKeys;
    private final List<Symbol> outputs;
    private final List<DataType> outputTypes;
    private final int limit;

    public ESGet(int executionPhaseId,
                 DocTableInfo tableInfo,
                 QuerySpec querySpec,
                 int limit,
                 UUID jobId) {

        assert querySpec.where().docKeys().isPresent();
        this.tableInfo = tableInfo;
        this.querySpec = querySpec;
        this.outputs = querySpec.outputs();
        this.docKeys = querySpec.where().docKeys().get();
        this.executionPhaseId = executionPhaseId;
        this.jobId = jobId;
        this.limit = limit;

        outputTypes = Symbols.extractTypes(outputs);

        if (querySpec.orderBy().isPresent()) {
            this.sortSymbols = querySpec.orderBy().get().orderBySymbols();
            this.reverseFlags = querySpec.orderBy().get().reverseFlags();
            this.nullsFirst = querySpec.orderBy().get().nullsFirst();
        } else {
            this.sortSymbols = ImmutableList.<Symbol>of();
            this.reverseFlags = EMPTY_REVERSE_FLAGS;
            this.nullsFirst = EMPTY_NULLS_FIRST;
        }
    }

    public List<DataType> outputTypes() {
        return outputTypes;
    }

    public DocTableInfo tableInfo() {
        return tableInfo;
    }

    public QuerySpec querySpec() {
        return querySpec;
    }

    public DocKeys docKeys() {
        return docKeys;
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return querySpec().offset();
    }

    public List<Symbol> sortSymbols() {
        return sortSymbols;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public Boolean[] nullsFirst() {
        return nullsFirst;
    }

    public int executionPhaseId() {
        return executionPhaseId;
    }

    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("docKeys", docKeys)
            .add("outputs", outputs)
            .toString();
    }

    @Override
    public void addProjection(Projection projection) {
        throw new UnsupportedOperationException("ESGetNode doesn't support projections");
    }

    @Override
    public boolean resultIsDistributed() {
        return false;
    }

    @Override
    public UpstreamPhase resultPhase() {
        //return this;
        throw new UnsupportedOperationException("resultPhase is not supported");
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitGetPlan(this, context);
    }

    @Override
    public UUID jobId() {
        return jobId;
    }
}

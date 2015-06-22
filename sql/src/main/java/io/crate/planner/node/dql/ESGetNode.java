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
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.where.DocKeys;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.IterablePlan;
import io.crate.planner.Plan;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Symbols;
import org.elasticsearch.common.Nullable;

import java.util.List;
import java.util.Set;
import java.util.UUID;


public class ESGetNode extends ESDQLPlanNode implements DQLPlanNode {

    private final TableInfo tableInfo;
    private final QuerySpec querySpec;
    private final List<Symbol> sortSymbols;
    private final boolean[] reverseFlags;
    private final Boolean[] nullsFirst;
    private final int executionNodeId;
    private final UUID jobId;

    private final static boolean[] EMPTY_REVERSE_FLAGS = new boolean[0];
    private final static Boolean[] EMPTY_NULLS_FIRST = new Boolean[0];
    private final DocKeys docKeys;

    public ESGetNode(int executionNodeId,
                     TableInfo tableInfo,
                     QuerySpec querySpec,
                     UUID jobId) {

        assert querySpec.where().docKeys().isPresent();
        this.tableInfo = tableInfo;
        this.querySpec = querySpec;
        this.outputs = querySpec.outputs();
        this.docKeys = querySpec.where().docKeys().get();
        this.executionNodeId = executionNodeId;
        this.jobId = jobId;


        outputTypes(Symbols.extractTypes(outputs));

        OrderBy orderBy = querySpec.orderBy();
        if (orderBy != null && orderBy.isSorted()){
            this.sortSymbols = orderBy.orderBySymbols();
            this.reverseFlags = orderBy.reverseFlags();
            this.nullsFirst = orderBy.nullsFirst();
        } else {
            this.sortSymbols = ImmutableList.<Symbol>of();
            this.reverseFlags = EMPTY_REVERSE_FLAGS;
            this.nullsFirst = EMPTY_NULLS_FIRST;
        }
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitESGetNode(this, context);
    }

    public TableInfo tableInfo() {
        return tableInfo;
    }

    public QuerySpec querySpec() {
        return querySpec;
    }

    public DocKeys docKeys() {
        return docKeys;
    }

    @Nullable
    public Integer limit() {
        return querySpec().limit();
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

    public int executionNodeId() {
        return executionNodeId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("docKeys", docKeys)
                .add("outputs", outputs)
                .toString();
    }

    public UUID jobId() {
        return jobId;
    }

    @Override
    public Plan plan() {
        return new IterablePlan(jobId, this);
    }
}

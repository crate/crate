/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.node.dql;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class TableFunctionCollectPhase extends RoutedCollectPhase implements CollectPhase {

    private final TableFunctionRelation relation;

    TableFunctionCollectPhase(UUID jobId,
                              int phaseId,
                              Routing routing,
                              TableFunctionRelation relation,
                              List<Projection> projections,
                              List<Symbol> outputs,
                              WhereClause whereClause) {
        super(jobId,
            phaseId,
            relation.function().info().ident().name(),
            routing,
            RowGranularity.DOC,
            outputs,
            projections,
            whereClause,
            DistributionInfo.DEFAULT_BROADCAST,
            null);
        this.relation = relation;
    }

    public TableFunctionRelation relation() {
        return relation;
    }

    @Override
    public Type type() {
        return Type.TABLE_FUNCTION_COLLECT;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitTableFunctionCollect(this, context);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // current table functions can be executed on the handler - no streaming required
        throw new UnsupportedOperationException("NYI");
    }
}

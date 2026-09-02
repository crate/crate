/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.replication.logical.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.elasticsearch.action.admin.cluster.snapshots.restore.TableOrPartition;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.replication.logical.action.CreatePublicationRequest;
import io.crate.replication.logical.action.TransportCreatePublication;
import io.crate.replication.logical.analyze.AnalyzedCreatePublication;

public class CreatePublicationPlan implements Plan {

    private final AnalyzedCreatePublication analyzedCreatePublication;

    public CreatePublicationPlan(AnalyzedCreatePublication analyzedCreatePublication) {
        this.analyzedCreatePublication = analyzedCreatePublication;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params, SubQueryResults subQueryResults) throws Exception {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            plannerContext.transactionContext(),
            dependencies.nodeContext(),
            x,
            params,
            subQueryResults
        );
        List<TableOrPartition> targets = new ArrayList<>(analyzedCreatePublication.tables().size());
        var sessionSettings = plannerContext.transactionContext().sessionSettings();
        for (var table : analyzedCreatePublication.tables()) {
            DocTableInfo tableInfo = dependencies.schemas().findRelation(
                table.getName(),
                Operation.CREATE_PUBLICATION,
                sessionSettings.sessionUser(),
                sessionSettings.searchPath()
            );
            String partitionIdent = null;
            if (table.partitionProperties().isEmpty() == false) {
                var partitionProperties = Lists.map(table.partitionProperties(), x -> x.map(eval));
                partitionIdent = PartitionName.ofAssignments(
                    tableInfo,
                    partitionProperties,
                    plannerContext.clusterState().metadata()
                ).ident();
            }
            targets.add(new TableOrPartition(tableInfo.ident(), partitionIdent));
        }
        var request = new CreatePublicationRequest(
            sessionSettings.sessionUser().name(),
            analyzedCreatePublication.name(),
            analyzedCreatePublication.isForAllTables(),
            targets
        );

        dependencies.client().execute(TransportCreatePublication.ACTION, request)
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1L : 1L)));
    }
}

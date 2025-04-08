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

package io.crate.planner;

import static java.util.Collections.emptyList;

import java.util.Collections;
import java.util.Objects;

import org.elasticsearch.action.support.master.AcknowledgedResponse;

import io.crate.analyze.AnalyzedSwapTable;
import io.crate.analyze.SwapTableAnalyzer;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.ddl.RelationNameSwap;
import io.crate.execution.ddl.SwapRelationsRequest;
import io.crate.execution.ddl.TransportSwapRelations;
import io.crate.execution.support.OneRowActionListener;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.DataTypes;

public class SwapTablePlan implements Plan {

    private final AnalyzedSwapTable swapTable;

    SwapTablePlan(AnalyzedSwapTable swapTable) {
        this.swapTable = swapTable;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        boolean dropSource = Objects.requireNonNull(
            DataTypes.BOOLEAN.sanitizeValue(SymbolEvaluator.evaluate(
                plannerContext.transactionContext(),
                dependencies.nodeContext(),
                swapTable.dropSource(),
                params,
                subQueryResults
            )), SwapTableAnalyzer.DROP_SOURCE + " option must be true or false, not null");

        RelationName source = swapTable.source().ident();
        SwapRelationsRequest request = new SwapRelationsRequest(
            Collections.singletonList(new RelationNameSwap(source, swapTable.target().ident())),
            dropSource ? Collections.singletonList(source) : emptyList()
        );
        OneRowActionListener<AcknowledgedResponse> listener = new OneRowActionListener<>(
            consumer,
            r -> r.isAcknowledged() ? new Row1(1L) : new Row1(0L)
        );
        dependencies.client().execute(TransportSwapRelations.ACTION, request).whenComplete(listener);
    }
}

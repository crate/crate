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

package io.crate.planner.node.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.jetbrains.annotations.VisibleForTesting;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.ddl.tables.DropPartitionsRequest;
import io.crate.execution.ddl.tables.TransportDropPartitionsAction;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.DataTypes;

public class DeletePartitions implements Plan {

    private final RelationName relationName;
    private final List<List<Symbol>> partitions;

    public DeletePartitions(RelationName relationName, List<List<Symbol>> partitions) {
        this.relationName = relationName;
        this.partitions = partitions;
    }

    @VisibleForTesting
    public List<List<Symbol>> partitions() {
        return partitions;
    }

    @Override
    public StatementType type() {
        return StatementType.DELETE;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        List<PartitionName> partitionNames = getPartitions(
            relationName, plannerContext.transactionContext(), dependencies.nodeContext(), params, subQueryResults);
        dependencies.client().execute(
            TransportDropPartitionsAction.ACTION,
            new DropPartitionsRequest(relationName, partitionNames)
        ).whenComplete(new OneRowActionListener<>(consumer, ignoredResponse -> Row1.ROW_COUNT_UNKNOWN));
    }

    @VisibleForTesting
    List<PartitionName> getPartitions(RelationName relationName,
                                      TransactionContext txnCtx,
                                      NodeContext nodeCtx,
                                      Row parameters,
                                      SubQueryResults subQueryResults) {
        List<PartitionName> values = new ArrayList<>();
        Function<Symbol, String> symbolToString =
            s -> DataTypes.STRING.implicitCast(SymbolEvaluator.evaluate(txnCtx, nodeCtx, s, parameters, subQueryResults));
        for (List<Symbol> partitionValues : partitions) {
            values.add(new PartitionName(relationName, Lists.map(partitionValues, symbolToString)));
        }
        return values;
    }
}

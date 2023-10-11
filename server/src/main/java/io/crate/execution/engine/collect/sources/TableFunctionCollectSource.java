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

package io.crate.execution.engine.collect.sources;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.TableFunctionCollectPhase;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.RowCollectExpression;
import io.crate.execution.engine.collect.ValueAndInputRow;
import io.crate.expression.InputCondition;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.RowType;

@Singleton
public class TableFunctionCollectSource implements CollectSource {

    private final NodeContext nodeCtx;
    private final InputFactory inputFactory;

    @Inject
    public TableFunctionCollectSource(NodeContext nodeCtx) {
        this.nodeCtx = nodeCtx;
        inputFactory = new InputFactory(nodeCtx);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<BatchIterator<Row>> getIterator(TransactionContext txnCtx,
                                                             CollectPhase collectPhase,
                                                             CollectTask collectTask,
                                                             boolean supportMoveToStart) {
        TableFunctionCollectPhase phase = (TableFunctionCollectPhase) collectPhase;
        TableFunctionImplementation<?> functionImplementation = phase.functionImplementation();
        RowType rowType = functionImplementation.returnType();

        //noinspection unchecked  Only literals can be passed to table functions. Anything else is invalid SQL
        List<Input<?>> inputs = (List<Input<?>>) (List<?>) phase.functionArguments();

        List<Input<?>> topLevelInputs = new ArrayList<>(phase.toCollect().size());
        List<String> columns = rowType.fieldNames();
        InputFactory.Context<RowCollectExpression> ctx = inputFactory.ctxForRefs(
            txnCtx,
            ref -> {
                for (int i = 0; i < columns.size(); i++) {
                    String column = columns.get(i);
                    if (ref.column().isRoot() && ref.column().name().equals(column)) {
                        return new RowCollectExpression(i);
                    }
                }
                throw new IllegalStateException(
                    "Column `" + ref + "` not found in " + functionImplementation.signature().getName().displayName());
            });
        for (Symbol symbol : phase.toCollect()) {
            topLevelInputs.add(ctx.add(symbol));
        }

        var inputRow = new ValueAndInputRow<>(topLevelInputs, ctx.expressions());
        Input<Boolean> condition = (Input<Boolean>) ctx.add(phase.where());
        Iterable<Row> rows = () -> StreamSupport
            .stream(functionImplementation.evaluate(txnCtx, nodeCtx, inputs.toArray(new Input[0])).spliterator(), false)
            .map(inputRow)
            .filter(InputCondition.asPredicate(condition))
            .iterator();
        return CompletableFuture.completedFuture(
            InMemoryBatchIterator.of(rows, SentinelRow.SENTINEL, functionImplementation.hasLazyResultSet())
        );
    }
}

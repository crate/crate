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

package io.crate.execution.engine.collect.sources;

import com.google.common.collect.Iterables;
import io.crate.analyze.OrderBy;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.TableFunctionCollectPhase;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.execution.engine.collect.RowsTransformer;
import io.crate.execution.engine.collect.ValueAndInputRow;
import io.crate.expression.InputCondition;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

@Singleton
public class TableFunctionCollectSource implements CollectSource {

    private final InputFactory inputFactory;

    @Inject
    public TableFunctionCollectSource(Functions functions) {
        inputFactory = new InputFactory(functions);
    }

    @Override
    public BatchIterator<Row> getIterator(TransactionContext txnCtx,
                                          CollectPhase collectPhase,
                                          CollectTask collectTask,
                                          boolean supportMoveToStart) {
        TableFunctionCollectPhase phase = (TableFunctionCollectPhase) collectPhase;
        TableFunctionImplementation<?> functionImplementation = phase.functionImplementation();
        TableInfo tableInfo = functionImplementation.createTableInfo();

        //noinspection unchecked  Only literals can be passed to table functions. Anything else is invalid SQL
        List<Input<?>> inputs = (List<Input<?>>) (List) phase.functionArguments();
        List<Reference> columns = new ArrayList<>(tableInfo.columns());

        List<Input<?>> topLevelInputs = new ArrayList<>(phase.toCollect().size());
        InputFactory.Context<InputCollectExpression> ctx =
            inputFactory.ctxForRefs(txnCtx, i -> new InputCollectExpression(columns.indexOf(i)));
        for (Symbol symbol : phase.toCollect()) {
            topLevelInputs.add(ctx.add(symbol));
        }

        Bucket buckets = functionImplementation.evaluate(txnCtx, inputs.toArray(new Input[0]));
        Iterable<Row> rows = Iterables.transform(
            buckets,
            new ValueAndInputRow<>(topLevelInputs, ctx.expressions()));
        Input<Boolean> condition = (Input<Boolean>) ctx.add(phase.where());
        rows = Iterables.filter(rows, InputCondition.asPredicate(condition));
        OrderBy orderBy = phase.orderBy();
        if (orderBy != null) {
            rows = RowsTransformer.sortRows(Iterables.transform(rows, Row::materialize), phase);
        }
        return InMemoryBatchIterator.of(rows, SentinelRow.SENTINEL);
    }
}

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

package io.crate.operation.collect.sources;

import com.google.common.collect.Iterables;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.core.collections.Row;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.operation.Input;
import io.crate.operation.InputFactory;
import io.crate.operation.collect.*;
import io.crate.operation.projectors.InputCondition;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.TableFunctionCollectPhase;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Singleton
public class TableFunctionCollectSource implements CollectSource {

    private final ClusterService clusterService;
    private final Functions functions;
    private final InputFactory inputFactory;

    @Inject
    public TableFunctionCollectSource(ClusterService clusterService, Functions functions) {
        this.clusterService = clusterService;
        this.functions = functions;
        inputFactory = new InputFactory(functions);
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase collectPhase,
                                                    RowReceiver downstream,
                                                    JobCollectContext jobCollectContext) {
        TableFunctionCollectPhase phase = (TableFunctionCollectPhase) collectPhase;
        WhereClause whereClause = phase.whereClause();
        if (whereClause.noMatch()) {
            return Collections.singletonList(RowsCollector.empty(downstream));
        }
        TableFunctionImplementation tableFunctionSafe = functions.getTableFunctionSafe(phase.functionName());
        TableInfo tableInfo = tableFunctionSafe.createTableInfo(clusterService, Symbols.extractTypes(phase.arguments()));
        //noinspection unchecked  Only literals can be passed to table functions. Anything else is invalid SQL
        List<Input<?>> inputs = (List<Input<?>>) (List) phase.arguments();
        List<Reference> columns = new ArrayList<>(tableInfo.columns());

        List<Input<?>> topLevelInputs = new ArrayList<>(phase.toCollect().size());
        InputFactory.Context<InputCollectExpression> ctx =
            inputFactory.ctxForRefs(i -> new InputCollectExpression(columns.indexOf(i)));
        for (Symbol symbol : phase.toCollect()) {
            topLevelInputs.add(ctx.add(symbol));
        }

        Iterable<Row> rows = Iterables.transform(
            tableFunctionSafe.execute(inputs),
            new ValueAndInputRow<>(topLevelInputs, ctx.expressions()));
        if (whereClause.hasQuery()) {
            Input<Boolean> condition = (Input<Boolean>) ctx.add(whereClause.query());
            rows = Iterables.filter(rows, InputCondition.asPredicate(condition));
        }
        OrderBy orderBy = phase.orderBy();
        if (orderBy != null) {
            rows = RowsTransformer.sortRows(Iterables.transform(rows, Row.MATERIALIZE), phase);
        }
        RowsCollector rowsCollector = new RowsCollector(downstream, rows);
        return Collections.<CrateCollector>singletonList(rowsCollector);
    }
}

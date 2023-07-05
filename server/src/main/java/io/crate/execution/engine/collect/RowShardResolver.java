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

package io.crate.execution.engine.collect;

import io.crate.analyze.Id;
import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;

import org.jetbrains.annotations.Nullable;
import io.crate.common.annotations.NotThreadSafe;

import org.elasticsearch.index.translog.Translog;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.crate.common.StringUtils.nullOrString;

@NotThreadSafe
public class RowShardResolver {

    private final Function<List<String>, String> idFunction;
    private final List<Input<?>> primaryKeyInputs;
    private final Input<?> routingInput;
    private final Iterable<CollectExpression<Row, ?>> expressions;

    private String id;
    private String routing;
    private List<String> pkValues;
    private long autoGeneratedTimestamp = Translog.UNSET_AUTO_GENERATED_TIMESTAMP;
    private boolean setAutoGeneratedTimestamp;


    public RowShardResolver(TransactionContext txnCtx,
                            NodeContext nodeCtx,
                            List<ColumnIdent> pkColumns,
                            List<? extends Symbol> primaryKeySymbols,
                            @Nullable ColumnIdent clusteredByColumn,
                            @Nullable Symbol routingSymbol) {
        InputFactory inputFactory = new InputFactory(nodeCtx);
        InputFactory.Context<CollectExpression<Row, ?>> context = inputFactory.ctxForInputColumns(txnCtx);
        idFunction = Id.compileWithNullValidation(pkColumns, clusteredByColumn);
        setAutoGeneratedTimestamp = pkColumns.size() == 1 && pkColumns.get(0).equals(DocSysColumns.ID);
        if (routingSymbol == null) {
            routingInput = null;
        } else {
            routingInput = context.add(routingSymbol);
        }
        primaryKeyInputs = new ArrayList<>(primaryKeySymbols.size());
        for (Symbol primaryKeySymbol : primaryKeySymbols) {
            primaryKeyInputs.add(context.add(primaryKeySymbol));
        }
        expressions = context.expressions();
    }


    public void setNextRow(Row row) {
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(row);
        }
        if (setAutoGeneratedTimestamp) {
            autoGeneratedTimestamp = Math.max(0, System.currentTimeMillis());
        }
        pkValues = pkValues(primaryKeyInputs);
        id = idFunction.apply(pkValues);
        if (routingInput == null) {
            routing = null;
        } else {
            // If clustered by column is specified it cannot be null.
            var clusteredBy = routingInput.value();
            if (clusteredBy == null) {
                throw new IllegalArgumentException("Clustered by value must not be NULL");
            }
            routing = clusteredBy.toString();
        }
    }

    private static List<String> pkValues(List<Input<?>> primaryKeyInputs) {
        return Lists2.map(primaryKeyInputs, input -> nullOrString(input.value()));
    }

    public long autoGeneratedTimestamp() {
        return autoGeneratedTimestamp;
    }

    public List<String> pkValues() {
        return pkValues;
    }

    /**
     * Returns the through collected inputs generated id
     */
    public String id() {
        return id;
    }

    /**
     * Returns the collected routing value (if available)
     */
    @Nullable
    public String routing() {
        return routing;
    }
}

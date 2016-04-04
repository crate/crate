/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.collect;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.analyze.Id;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Row;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.operation.BaseImplementationSymbolVisitor;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.Inputs;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;

@NotThreadSafe
public class RowShardResolver {

    private final com.google.common.base.Function<List<BytesRef>, String> idFunction;
    private final ImplementationSymbolVisitor.Context visitorContext;
    private final List<Input<?>> primaryKeyInputs;
    private final Input<?> routingInput;

    private String id;
    private String routing;


    public RowShardResolver(Functions functions,
                            List<ColumnIdent> pkColumns,
                            List<? extends Symbol> primaryKeySymbols,
                            @Nullable ColumnIdent clusteredByColumn,
                            @Nullable Symbol routingSymbol) {
        Visitor visitor = new Visitor(functions);
        idFunction = Id.compile(pkColumns, clusteredByColumn);
        visitorContext = new ImplementationSymbolVisitor.Context();
        routingInput = routingSymbol == null ? null : visitor.process(routingSymbol, visitorContext);
        primaryKeyInputs = new ArrayList<>(primaryKeySymbols.size());
        for (Symbol primaryKeySymbol : primaryKeySymbols) {
            primaryKeyInputs.add(visitor.process(primaryKeySymbol, visitorContext));
        }
    }


    public void setNextRow(Row row) {
        for (CollectExpression<Row, ?> expression : visitorContext.collectExpressions()) {
            expression.setNextRow(row);
        }
        id = idFunction.apply(pkValues(primaryKeyInputs));
        if (routingInput == null) {
            routing = null;
        } else {
            routing = BytesRefs.toString(routingInput.value());
        }
    }

    private List<BytesRef> pkValues(List<Input<?>> primaryKeyInputs) {
        if (primaryKeyInputs.isEmpty()) {
            return ImmutableList.of(); // avoid object creation in Lists.transform if the list is empty
        }
        return Lists.transform(primaryKeyInputs, Inputs.TO_BYTES_REF);
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

    static class Visitor extends BaseImplementationSymbolVisitor<ImplementationSymbolVisitor.Context> {

        public Visitor(Functions functions) {
            super(functions);
        }

        @Override
        public Input<?> visitInputColumn(InputColumn inputColumn, ImplementationSymbolVisitor.Context context) {
            return context.collectExpressionFor(inputColumn);
        }
    }
}

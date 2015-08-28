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
import io.crate.core.collections.Row;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.planner.symbol.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.ArrayList;
import java.util.List;

public class ShardingProjector {

    private static final com.google.common.base.Function<Input<?>, BytesRef> INPUT_BYTES_REF_FUNCTION =
            new com.google.common.base.Function<Input<?>, BytesRef>() {
        @javax.annotation.Nullable
        @Override
        public BytesRef apply(@Nullable Input<?> input) {
            if (input == null) {
                return null;
            }
            return BytesRefs.toBytesRef(input.value());
        }
    };

    private static final Visitor VISITOR = new Visitor();

    private final com.google.common.base.Function<List<BytesRef>, String> idFunction;
    private final ImplementationSymbolVisitor.Context visitorContext;
    private final List<Input<?>> primaryKeyInputs;
    private final Input<?> routingInput;

    private String id;
    private String routing;


    public ShardingProjector(List<ColumnIdent> pkColumns,
                             List<Symbol> primaryKeySymbols,
                             @Nullable ColumnIdent clusteredByColumn,
                             @Nullable Symbol routingSymbol) {

        idFunction = Id.compile(pkColumns, clusteredByColumn);
        visitorContext = new ImplementationSymbolVisitor.Context();
        routingInput = routingSymbol == null ? null : VISITOR.process(routingSymbol, visitorContext);
        primaryKeyInputs = new ArrayList<>(primaryKeySymbols.size());
        for (Symbol primaryKeySymbol : primaryKeySymbols) {
            primaryKeyInputs.add(VISITOR.process(primaryKeySymbol, visitorContext));
        }
    }


    public synchronized boolean setNextRow(Row row) {
        for (CollectExpression<Row, ?> expression : visitorContext.collectExpressions()) {
            expression.setNextRow(row);
        }
        id = idFunction.apply(pkValues(primaryKeyInputs));
        if (routingInput == null) {
            routing = null;
        } else {
            routing = BytesRefs.toString(routingInput.value());
        }
        return true;
    }

    private List<BytesRef> pkValues(List<Input<?>> primaryKeyInputs) {
        if (primaryKeyInputs.isEmpty()) {
            return ImmutableList.of(); // avoid object creation in Lists.transform if the list is empty
        }
        return Lists.transform(primaryKeyInputs, INPUT_BYTES_REF_FUNCTION);
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

    static class Visitor extends SymbolVisitor<ImplementationSymbolVisitor.Context, Input<?>> {

        @Override
        protected Input<?> visitSymbol(Symbol symbol, ImplementationSymbolVisitor.Context context) {
            throw new AssertionError("Symbol " + SymbolFormatter.format(symbol) + " not supported");
        }

        @Override
        public Input<?> visitInputColumn(InputColumn inputColumn, ImplementationSymbolVisitor.Context context) {
            return context.collectExpressionFor(inputColumn);
        }

        @Override
        public Input<?> visitLiteral(Literal symbol, ImplementationSymbolVisitor.Context context) {
            return symbol;
        }
    }
}

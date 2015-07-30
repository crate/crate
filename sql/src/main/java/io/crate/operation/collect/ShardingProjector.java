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

import com.google.common.collect.Lists;
import io.crate.analyze.Id;
import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.operation.*;
import io.crate.operation.projectors.Projector;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolFormatter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.ArrayList;
import java.util.List;

public class ShardingProjector implements Projector, RowDownstreamHandle {

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
    private final Visitor visitor;
    private final List<Symbol> primaryKeySymbols;
    @Nullable
    private final Symbol routingSymbol;
    private final com.google.common.base.Function<List<BytesRef>, String> idFunction;

    private VisitorContext visitorContext;
    private List<Input<?>> primaryKeyInputs;
    @Nullable
    private Input<?> routingInput;

    private String id;
    @Nullable
    private String routing;


    public ShardingProjector(List<ColumnIdent> pkColumns,
                             List<Symbol> primaryKeySymbols,
                             @Nullable ColumnIdent clusteredByColumn,
                             @Nullable Symbol routingSymbol) {

        idFunction = Id.compile(pkColumns, clusteredByColumn);
        visitor = new Visitor();
        this.primaryKeySymbols = primaryKeySymbols;
        this.routingSymbol = routingSymbol;
    }

    @Override
    public void startProjection(ExecutionState executionState) {
        visitorContext = new VisitorContext();
        if (routingSymbol != null ) {
            routingInput = visitor.process(routingSymbol, visitorContext);
        } else {
            routingInput = null;
        }
        primaryKeyInputs = new ArrayList<>(primaryKeySymbols.size());
        for (Symbol primaryKeySymbol : primaryKeySymbols) {
            primaryKeyInputs.add(visitor.process(primaryKeySymbol, visitorContext));
        }
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        assert visitorContext != null : "startProjection() must be called first";
        for (CollectExpression collectExpression : visitorContext.collectExpressions()) {
            collectExpression.setNextRow(row);
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
        return Lists.transform(primaryKeyInputs, INPUT_BYTES_REF_FUNCTION);
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        throw new UnsupportedOperationException("ShardingProjector does not support upstreams");
    }

    @Override
    public void finish() {
    }

    @Override
    public void fail(Throwable throwable) {
    }

    @Override
    public void downstream(RowDownstream downstream) {
        throw new UnsupportedOperationException("ShardingProjector does not support downstreams");
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

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(boolean async) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void repeat() {
    }

    static class VisitorContext extends ImplementationSymbolVisitor.Context {}

    static class Visitor extends ImplementationSymbolVisitor {

        public Visitor() {
            super(null, null, null);
        }

        @Override
        public Input<?> visitReference(Reference symbol, Context context) {
            throw new UnsupportedOperationException(SymbolFormatter.format("Cannot handle Reference %s", symbol));
        }

        @Override
        public Input<?> visitFunction(Function function, Context context) {
            throw new UnsupportedOperationException(SymbolFormatter.format("Can't handle Symbol %s", function));
        }

        @Override
        public Functions functions() {
            throw new AssertionError("Functions not supported here");
        }
    }
}

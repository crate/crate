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

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.projectors.Projector;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolFormatter;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShardingProjector implements Projector, ProjectorUpstream {

    private final Visitor visitor;
    private final List<Symbol> primaryKeySymbols;
    @Nullable
    private final Symbol routingSymbol;

    private VisitorContext visitorContext;
    private List<Input<?>> primaryKeyInputs;
    @Nullable
    private Input<?> routingInput;
    private boolean collectIdOrAutoGenerate;

    private String id;
    @Nullable
    private String routing;


    public ShardingProjector(List<ColumnIdent> primaryKeyIdents,
                             List<Symbol> primaryKeySymbols,
                             @Nullable Symbol routingSymbol) {
        visitor = new Visitor();
        this.primaryKeySymbols = primaryKeySymbols;
        this.routingSymbol = routingSymbol;
        if (primaryKeyIdents.size() == 1 && primaryKeyIdents.get(0).fqn().equals("_id")) {
            collectIdOrAutoGenerate = true;
        }
    }

    @Override
    public void startProjection() {
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
    public boolean setNextRow(Object... args) {
        assert visitorContext != null : "startProjection() must be called first";
        for (CollectExpression collectExpression : visitorContext.collectExpressions()) {
            collectExpression.setNextRow(args);
        }
        applyInputs(primaryKeyInputs, routingInput);
        return true;
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        throw new UnsupportedOperationException("ShardingProjector does not support upstreams");
    }

    @Override
    public void upstreamFinished() {
    }

    @Override
    public void upstreamFailed(Throwable throwable) {
    }

    @Override
    public void downstream(Projector downstream) {
        throw new UnsupportedOperationException("ShardingProjector does not support downstreams");
    }

    private void applyInputs(List<Input<?>> primaryKeyInputs, @Nullable Input<?> routingInput) {
        if (primaryKeyInputs.size() == 0) {
            id = Strings.base64UUID();
        } else if (primaryKeyInputs.size() == 1) {
            Object value = primaryKeyInputs.get(0).value();
            if (value == null && collectIdOrAutoGenerate) {
                id = Strings.base64UUID();
            } else if (value == null) {
                throw new IllegalArgumentException("A primary key value must not be NULL");
            } else {
                id = BytesRefs.toString(primaryKeyInputs.get(0).value());
            }
        } else {
            BytesStreamOutput out = new BytesStreamOutput();
            try {
                out.writeVInt(primaryKeyInputs.size());
                for (Input<?> input : primaryKeyInputs) {
                    Object value = input.value();
                    if (value == null) {
                        throw new IllegalArgumentException("A primary key value must not be NULL");
                    }
                    out.writeString(BytesRefs.toString(value));
                }
                out.close();
            } catch (IOException e) {
                //
            }
            id = Base64.encodeBytes(out.bytes().toBytes());
        }
        if (routingInput != null) {
            routing = BytesRefs.toString(routingInput.value());
        } else {
            routing = null;
        }
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

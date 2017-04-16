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

import io.crate.metadata.Functions;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolFormatter;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HandlerSideShardingCollectExpression extends CollectExpression<HandlerSideShardingCollectExpression.IdAndRouting> {

    private final Visitor visitor;
    private final List<Input<?>> primaryKeyInputs;
    private final Input<?> routingInput;
    private final VisitorContext visitorContext;

    private IdAndRouting value;

    public HandlerSideShardingCollectExpression(Functions functions,
                                                List<Symbol> primaryKeySymbols,
                                                @Nullable Symbol routingSymbol) {
        visitor = new Visitor(functions);
        visitorContext = new VisitorContext();
        if (routingSymbol != null) {
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
        for (CollectExpression collectExpression : visitorContext.collectExpressions()) {
            collectExpression.setNextRow(args);
        }
        value = new IdAndRouting(primaryKeyInputs, routingInput);
        return true;
    }

    @Override
    public IdAndRouting value() {
        return value;
    }

    public static class IdAndRouting {

        private final String id;
        @Nullable
        private String routing;

        public IdAndRouting(List<Input<?>> primaryKeyInputs, @Nullable Input<?> routingInput) {
            if (primaryKeyInputs.size() == 0) {
                id = Strings.base64UUID();
            } else if (primaryKeyInputs.size() == 1) {
                id = primaryKeyInputs.get(0).value().toString();
            } else {
                BytesStreamOutput out = new BytesStreamOutput();
                try {
                    out.writeVInt(primaryKeyInputs.size());
                    for (Input<?> input : primaryKeyInputs) {
                        out.writeString(input.value().toString());
                    }
                    out.close();
                } catch (IOException e) {
                    //
                }
                id = Base64.encodeBytes(out.bytes().toBytes());
            }
            if (routingInput != null) {
                routing = routingInput.value().toString();
            }
        }

        public String id() {
            return id;
        }

        @Nullable
        public String routing() {
            return routing;
        }

    }

    static class VisitorContext extends ImplementationSymbolVisitor.Context {}

    static class Visitor extends ImplementationSymbolVisitor {

        public Visitor(Functions functions) {
            super(null, functions, null);
        }

        @Override
        public Input<?> visitReference(Reference symbol, Context context) {
            throw new IllegalArgumentException(SymbolFormatter.format("Cannot handle Reference %s", symbol));
        }
    }
}

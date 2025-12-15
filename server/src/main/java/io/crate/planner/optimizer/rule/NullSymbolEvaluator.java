/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.optimizer.rule;

import io.crate.data.Input;
import io.crate.expression.BaseImplementationSymbolVisitor;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.OuterColumn;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.ScopedRef;
import io.crate.metadata.TransactionContext;

final class NullSymbolEvaluator extends BaseImplementationSymbolVisitor<Void> {

    public NullSymbolEvaluator(TransactionContext txnCtx, NodeContext nodeCtx) {
        super(txnCtx, nodeCtx);
    }

    @Override
    public Input<?> visitField(ScopedSymbol field, Void context) {
        return Literal.NULL;
    }

    @Override
    public Input<?> visitReference(ScopedRef symbol, Void context) {
        return Literal.NULL;
    }

    @Override
    public Input<?> visitParameterSymbol(ParameterSymbol parameterSymbol, Void context) {
        return Literal.NULL;
    }

    @Override
    public Input<?> visitSelectSymbol(SelectSymbol selectSymbol, Void context) {
        return Literal.NULL;
    }

    @Override
    public Input<?> visitOuterColumn(OuterColumn outerColumn, Void context) {
        return Literal.NULL;
    }
}

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

package io.crate.expression.symbol;

import io.crate.analyze.FrameBoundDefinition;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;

public abstract class DefaultTraversalSymbolVisitor<C, R> extends SymbolVisitor<C, R> {

    /*
     * These methods don't call into super to enable subclasses to use overwrite `visitSymbol` to visit all leaves.
     * If we called super.visitXY here,
     * it would cause visitSymbol to be called for Function, FetchReference and MatchPredicate as well.
     */

    @Override
    public R visitFunction(Function symbol, C context) {
        for (Symbol arg : symbol.arguments()) {
            arg.accept(this, context);
        }
        var filter = symbol.filter();
        if (filter != null) {
            filter.accept(this, context);
        }
        return null;
    }

    @Override
    public R visitCast(CastSymbol castSymbol, C context) {
        castSymbol.symbol().accept(this, context);
        return null;
    }

    @Override
    public R visitWindowFunction(WindowFunction symbol, C context) {
        for (Symbol arg : symbol.arguments()) {
            arg.accept(this, context);
        }
        Symbol filter = symbol.filter();
        if (filter != null) {
            filter.accept(this, context);
        }
        WindowDefinition windowDefinition = symbol.windowDefinition();
        OrderBy orderBy = windowDefinition.orderBy();
        if (orderBy != null) {
            for (Symbol orderBySymbol : orderBy.orderBySymbols()) {
                orderBySymbol.accept(this, context);
            }
        }
        for (Symbol partition : windowDefinition.partitions()) {
            partition.accept(this, context);
        }

        Symbol frameStartValueSymbol = windowDefinition.windowFrameDefinition().start().value();
        if (frameStartValueSymbol != null) {
            frameStartValueSymbol.accept(this, context);
        }

        FrameBoundDefinition end = windowDefinition.windowFrameDefinition().end();
        if (end != null) {
            Symbol frameEndValueSymbol = end.value();
            if (frameEndValueSymbol != null) {
                frameEndValueSymbol.accept(this, context);
            }
        }

        return null;
    }

    @Override
    public R visitFetchReference(FetchReference fetchReference, C context) {
        fetchReference.fetchId().accept(this, context);
        fetchReference.ref().accept(this, context);
        return null;
    }

    @Override
    public R visitMatchPredicate(MatchPredicate matchPredicate, C context) {
        for (Symbol field : matchPredicate.identBoostMap().keySet()) {
            field.accept(this, context);
        }
        matchPredicate.queryTerm().accept(this, context);
        matchPredicate.options().accept(this, context);
        return null;
    }

    @Override
    public R visitAlias(AliasSymbol aliasSymbol, C context) {
        aliasSymbol.symbol().accept(this, context);
        return null;
    }
}

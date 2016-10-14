/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.symbol;

import io.crate.metadata.Reference;

import javax.annotation.Nullable;


public class SymbolVisitor<C, R> {

    public R process(Symbol symbol, @Nullable C context) {
        return symbol.accept(this, context);
    }

    protected R visitSymbol(Symbol symbol, C context) {
        return null;
    }

    public R visitAggregation(Aggregation symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitReference(Reference symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitDynamicReference(DynamicReference symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitFunction(Function symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitLiteral(Literal symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitInputColumn(InputColumn inputColumn, C context) {
        return visitSymbol(inputColumn, context);
    }

    public R visitRelationColumn(RelationColumn relationColumn, C context) {
        return visitSymbol(relationColumn, context);
    }

    public R visitValue(Value symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitField(Field field, C context) {
        return visitSymbol(field, context);
    }

    public R visitMatchPredicate(MatchPredicate matchPredicate, C context) {
        return visitSymbol(matchPredicate, context);
    }

    public R visitFetchReference(FetchReference fetchReference, C context) {
        return visitSymbol(fetchReference, context);
    }

    public R visitParameterSymbol(ParameterSymbol parameterSymbol, C context) {
        return visitSymbol(parameterSymbol, context);
    }

    public R visitSelectSymbol(SelectSymbol selectSymbol, C context) {
        return visitSymbol(selectSymbol, context);
    }
}


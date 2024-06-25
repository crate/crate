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

import java.io.IOException;
import java.util.function.Predicate;

import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.symbol.format.Style;
import io.crate.types.DataType;

/**
 * Used to represents columns from an outer relation in a correlated subquery.
 *
 * <pre>
 * Example:
 * </pre>
 *
 * <pre>
 * {@code
 * SELECT (SELECT t.mountains) FROM sys.summits t
 *                ^^^^^^^^^^^
 *                OuterColumn
 *                 symbol: ScopedSymbol(relationName=t, column=mountains)
 *                 relation: AliasedAnalyzedRelation(t)
 * }
 * </pre>
 **/
public final class OuterColumn implements Symbol {

    private final AnalyzedRelation relation;
    private final Symbol symbol;

    public OuterColumn(AnalyzedRelation relation, Symbol symbol) {
        this.relation = relation;
        this.symbol = symbol;
    }

    public AnalyzedRelation relation() {
        return relation;
    }

    public Symbol symbol() {
        return symbol;
    }

    @Override
    public boolean isDeterministic() {
        return symbol.isDeterministic();
    }

    @Override
    public boolean any(Predicate<? super Symbol> predicate) {
        return predicate.test(this) || symbol.any(predicate);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Cannot stream OuterColumn symbol " + toString());
    }

    @Override
    public SymbolType symbolType() {
        return symbol.symbolType();
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitOuterColumn(this, context);
    }

    @Override
    public DataType<?> valueType() {
        return symbol.valueType();
    }

    @Override
    public String toString() {
        return symbol.toString();
    }

    @Override
    public String toString(Style style) {
        return symbol.toString(style);
    }

    @Override
    public long ramBytesUsed() {
        return symbol.ramBytesUsed();
    }
}

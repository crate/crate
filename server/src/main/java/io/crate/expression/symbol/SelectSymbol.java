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

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.symbol.format.Style;
import io.crate.types.ArrayType;
import io.crate.types.DataType;

/**
 * Symbol representing a sub-query
 */
public class SelectSymbol implements Symbol {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SelectSymbol.class);

    private final AnalyzedRelation relation;
    private final ArrayType<?> dataType;
    private final ResultType resultType;
    private final boolean isCorrelated;
    private final boolean parentIsOrderSensitive;

    public enum ResultType {
        SINGLE_COLUMN_SINGLE_VALUE,
        SINGLE_COLUMN_MULTIPLE_VALUES,
        SINGLE_COLUMN_EXISTS
    }

    public SelectSymbol(AnalyzedRelation relation, ArrayType<?> dataType,
                        ResultType resultType,
                        boolean parentIsOrderSensitive) {

        this.relation = relation;
        this.dataType = dataType;
        this.resultType = resultType;
        boolean[] isCorrelatedArr = new boolean[] { false };
        relation.visitSymbols(symbolTree -> {
            if (SymbolVisitors.any(s -> s instanceof OuterColumn, symbolTree)) {
                isCorrelatedArr[0] = true;
            }
        });
        this.isCorrelated = isCorrelatedArr[0];
        this.parentIsOrderSensitive = parentIsOrderSensitive;
    }

    public AnalyzedRelation relation() {
        return relation;
    }

    public boolean isCorrelated() {
        return isCorrelated;
    }

    public boolean parentIsOrderSensitive() {
        return parentIsOrderSensitive;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Cannot stream SelectSymbol");
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.SELECT_SYMBOL;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitSelectSymbol(this, context);
    }

    @Override
    public DataType<?> valueType() {
        if (resultType == ResultType.SINGLE_COLUMN_SINGLE_VALUE) {
            return dataType.innerType();
        }
        return dataType;
    }

    @Override
    public String toString() {
        return "(" + relation + ")";
    }

    @Override
    public String toString(Style style) {
        return "(" + relation + ")";
    }

    public ResultType getResultType() {
        return resultType;
    }

    @Override
    public long ramBytesUsed() {
        // Missing the size of the relation, but SelectSymbol isn't used inplaces
        // where we batch a large amount of symbols, so this should be good enough.
        return SHALLOW_SIZE + dataType.ramBytesUsed();
    }
}

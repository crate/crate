/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.symbol;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.types.DataType;
import io.crate.types.RowType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Symbol representing a sub-query
 */
public class SelectSymbol extends Symbol {

    private final AnalyzedRelation relation;
    private final RowType type;

    public SelectSymbol(AnalyzedRelation relation, RowType type) {
        this.relation = relation;
        this.type = type;
    }

    public AnalyzedRelation relation() {
        return relation;
    }

    public static final SymbolFactory FACTORY = new SymbolFactory() {
        @Override
        public Symbol newInstance() {
            throw new UnsupportedOperationException("Cannot stream SelectSymbol");
        }
    };

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("Cannot stream SelectSymbol");
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
    public DataType valueType() {
        return type;
    }

    @Override
    public String toString() {
        return "SelectSymbol{" + type.toString() + "}";
    }
}

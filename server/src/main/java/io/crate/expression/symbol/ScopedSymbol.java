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

import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Constants;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.types.DataType;

/**
 * A symbol that associates another symbol with a relation.
 *
 * <pre>
 * {@code
 *                            DocTableRelation
 *                              |    outputs: [Reference(x)]
 *                              |
 *      SELECT t1.x, t2.x FROM tbl AS t1, tbl AS t2
 *              |              ~~~~~~~~~
 *              |                 AliasedAnalyzedRelation;
 *              |                       puts Reference(x) into the ScopedSymbol to associate it to itself.
 *              |
 *            ScopedSymbol
 *              relation = t1
 *
 * }
 * </pre>
 */
public final class ScopedSymbol implements Symbol {

    private final RelationName relation;
    private final ColumnIdent column;
    private final DataType<?> dataType;

    /**
     * Used to disambiguate columns with the same name.
     **/
    private final int identity;

    /**
     * <p>
     * Creates a ScopedSymbol describing output from a relation.
     * E.g. in the case of UNION its a stand-in for two symbols from the two child
     * relations.
     * </p>
     *
     * Use the {@link #ScopedSymbol(RelationName, ColumnIdent, Symbol)} constructor
     * if the symbol is pointing to a single specific symbol of a child relation
     *
     * @param relation owner of this symbol
     * @param column name of the symbol
     **/
    public ScopedSymbol(RelationName relation, ColumnIdent column, DataType<?> dataType) {
        this.relation = relation;
        this.column = column;
        this.dataType = dataType;
        this.identity = 0;
    }

    /**
     * @param relation owner of this symbol
     * @param column name of the symbol
     * @param symbol the symbol this ScopedSymbol is pointing towards
     **/
    public ScopedSymbol(RelationName relation, ColumnIdent column, Symbol symbol) {
        this.relation = relation;
        this.column = column;
        this.dataType = symbol.valueType();
        this.identity = System.identityHashCode(symbol);
    }

    public RelationName relation() {
        return relation;
    }

    public ColumnIdent column() {
        return column;
    }

    @Override
    public ColumnIdent toColumn() {
        return column;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.RELATION_OUTPUT;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitField(this, context);
    }

    @Override
    public DataType<?> valueType() {
        return dataType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException(
            "ScopedSymbol `" + toString(Style.QUALIFIED) + "` is not streamable. " +
            "This error is a bug. Please create an issue in " + Constants.ISSUE_URL);
    }

    @Override
    public String toString() {
        return column.quotedOutputName();
    }

    @Override
    public String toString(Style style) {
        if (style == Style.QUALIFIED) {
            return relation.sqlFqn() + '.' + column.quotedOutputName();
        }
        return column.quotedOutputName();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ScopedSymbol other
            && relation.equals(other.relation)
            && column.equals(other.column)
            && dataType.equals(other.dataType)
            && identity == other.identity;
    }

    @Override
    public int hashCode() {
        int result = relation.hashCode();
        result = 31 * result + column.hashCode();
        result = 31 * result + dataType.hashCode();
        result = 31 * result + identity;
        return result;
    }

    @Override
    public long ramBytesUsed() {
        return dataType.ramBytesUsed();
    }
}

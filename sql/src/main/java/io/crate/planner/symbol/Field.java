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

package io.crate.planner.symbol;

import com.google.common.base.MoreObjects;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Field extends Symbol {

    public static final SymbolFactory<Field> FACTORY = new SymbolFactory<Field>() {
        @Override
        public Field newInstance() {
            return new Field();
        }
    };

    /**
     * If the whereClause has a query it will create a new WhereClause with the query unwrapped.
     * See {@link #unwrap(Symbol symbol)}
     */
    @Deprecated
    public static WhereClause unwrap(WhereClause whereClause) {
        if (whereClause.hasQuery()) {
            return new WhereClause(unwrap(whereClause.query()));
        }
        return whereClause;
    }

    /**
     * Will unwrap all {@link io.crate.planner.symbol.Field} symbols to their target.
     * This will also traverse functions and re-write them to unwrap all Fields that are used as arguments.
     *
     * Deprecated as this is just a migration helper. The unwrapping should later be done in a normalization step
     * that is relation specific.
     */
    @Deprecated
    public static Symbol unwrap(Symbol symbol) {
        if (symbol == null) {
            return null;
        }
        return UNWRAPPING_VISITOR.process(symbol, null);
    }

    /**
     * Will create a new list where each symbol is unwrapped.
     * See {@link #unwrap{Symbol symbol}
     */
    @Deprecated
    public static List<Symbol> unwrap(Collection<? extends Symbol> symbols) {
        List<Symbol> unwrapped = new ArrayList<>(symbols.size());
        for (Symbol symbol : symbols) {
            unwrapped.add(unwrap(symbol));
        }
        return unwrapped;
    }

    private static final UnwrappingVisitor UNWRAPPING_VISITOR = new UnwrappingVisitor();
    private AnalyzedRelation relation;
    private String name;
    private Symbol target;

    public Field(AnalyzedRelation relation, String name, Symbol target) {
        this.relation = relation;
        this.name = name;
        this.target = target;
    }

    private Field() {}

    public String name() {
        return name;
    }

    public AnalyzedRelation relation() {
        return relation;
    }

    @Deprecated
    public Symbol target() {
        return target;
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
    public DataType valueType() {
        return target.valueType();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("RelationOutput is not streamable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("RelationOutput is not streamable");
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(Field.class).add("target", target).add("relation", relation).toString();
    }

    private static class UnwrappingVisitor extends SymbolVisitor<Void, Symbol>
    {
        @Override
        public Symbol visitField(Field field, Void context) {
            return process(field.target(), context);
        }

        @Override
        public Symbol visitFunction(Function symbol, Void context) {
            for (int i = 0; i < symbol.arguments().size(); i++) {
                symbol.setArgument(i, process(symbol.arguments().get(i), context));
            }
            return symbol;
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, Void context) {
            return symbol;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Field that = (Field) o;

        if (!relation.equals(that.relation)) return false;
        if (!target.equals(that.target)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = relation.hashCode();
        result = 31 * result + target.hashCode();
        return result;
    }
}

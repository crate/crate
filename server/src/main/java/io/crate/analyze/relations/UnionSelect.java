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

package io.crate.analyze.relations;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;
import org.elasticsearch.common.UUIDs;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class UnionSelect implements AnalyzedRelation {

    private final AnalyzedRelation left;
    private final AnalyzedRelation right;
    private final List<ScopedSymbol> outputs;
    private final RelationName name;

    public UnionSelect(AnalyzedRelation left, AnalyzedRelation right) {
        assert left.outputs().size() == right.outputs().size()
            : "Both the left side and the right side of UNION must have the same number of outputs";
        this.left = left;
        this.right = right;
        this.name = new RelationName(null, UUIDs.randomBase64UUID());
        // SQL semantics dictate that UNION uses the column names from the first relation (top or left side)
        List<Symbol> fieldsFromLeft = left.outputs();
        ArrayList<ScopedSymbol> outputs = new ArrayList<>(fieldsFromLeft.size());
        for (Symbol field : fieldsFromLeft) {
            outputs.add(new ScopedSymbol(name, Symbols.pathFromSymbol(field), field.valueType()));
        }
        this.outputs = List.copyOf(outputs);
    }

    public AnalyzedRelation left() {
        return left;
    }

    public AnalyzedRelation right() {
        return right;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitUnionSelect(this, context);
    }

    @Override
    public Symbol getField(ColumnIdent column, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        for (var output : outputs) {
            if (output.column().equals(column)) {
                return output;
            }
        }
        return null;
    }

    @Override
    public RelationName relationName() {
        return name;
    }

    @Nonnull
    @Override
    public List<Symbol> outputs() {
        return (List<Symbol>)(List) outputs;
    }

    @Override
    public String toString() {
        return left + " UNION ALL " + right;
    }
}

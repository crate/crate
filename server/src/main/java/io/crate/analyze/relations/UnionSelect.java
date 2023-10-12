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

import static io.crate.types.DataTypes.merge;

import java.util.ArrayList;
import java.util.List;

import org.jetbrains.annotations.NotNull;

import org.elasticsearch.common.UUIDs;

import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;

public class UnionSelect implements AnalyzedRelation {

    private final AnalyzedRelation left;
    private final AnalyzedRelation right;
    private final List<ScopedSymbol> outputs;
    private final RelationName name;
    private final boolean isDistinct;

    public UnionSelect(AnalyzedRelation left, AnalyzedRelation right, boolean isDistinct) {
        if (left.outputs().size() != right.outputs().size()) {
            throw new UnsupportedOperationException("Number of output columns must be the same for all parts of a UNION");
        }
        this.left = left;
        this.right = right;
        this.name = new RelationName(null, UUIDs.dirtyUUID().toString());
        // SQL semantics dictate that UNION uses the column names from the first relation (top or left side)
        var leftOutputs = left.outputs();
        var rightOutputs = right.outputs();
        ArrayList<ScopedSymbol> outputs = new ArrayList<>(leftOutputs.size());
        for (int i = 0; i < leftOutputs.size(); i++) {
            var l = leftOutputs.get(i);
            var r = rightOutputs.get(i);
            try {
                outputs.add(new ScopedSymbol(name, Symbols.pathFromSymbol(l), merge(l.valueType(), r.valueType())));
            } catch (IllegalArgumentException e) {
                throw new UnsupportedOperationException(
                    "Output columns at position " + (i + 1) +
                    " must be compatible for all parts of a UNION. " +
                    "Got `" + l.valueType().getName() + "` and `" + r.valueType().getName() + "`");
            }
        }
        this.outputs = List.copyOf(outputs);
        this.isDistinct = isDistinct;
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
    public Symbol getField(ColumnIdent column, Operation operation, boolean errorOnUnknownObjectKey) throws AmbiguousColumnException, ColumnUnknownException, UnsupportedOperationException {
        Symbol field = null;
        for (var output : outputs) {
            if (output.column().equals(column)) {
                if (field != null) {
                    throw new AmbiguousColumnException(output.column(), output);
                }
                field = output;
            }
        }
        return field;
    }

    @Override
    public RelationName relationName() {
        return name;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @NotNull
    @Override
    public List<Symbol> outputs() {
        return (List<Symbol>)(List) outputs;
    }

    @Override
    public String toString() {
        return left + " UNION " + (isDistinct ? "DISTINCT " : "ALL ") + right;
    }

    public boolean isDistinct() {
        return isDistinct;
    }
}

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
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

public class UnionSelect implements AnalyzedRelation {

    private final AnalyzedRelation left;
    private final AnalyzedRelation right;
    private final List<ScopedSymbol> outputs;
    private final RelationName name;
    private final boolean isDistinct;

    public UnionSelect(AnalyzedRelation left, AnalyzedRelation right, boolean isDistinct) {
        assert left.outputs().size() == right.outputs().size()
            : "Both the left side and the right side of UNION must have the same number of outputs";
        this.left = left;
        this.right = right;
        this.name = new RelationName(null, UUIDs.dirtyUUID().toString());
        // SQL semantics dictate that UNION uses the column names from the first relation (top or left side)
        this.outputs = List.copyOf(unionizeOutputs(left.outputs(), right.outputs()));
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

    private List<ScopedSymbol> unionizeOutputs(List<Symbol> left, List<Symbol> right) {
        ArrayList<ScopedSymbol> outputs = new ArrayList<>(left.size());
        for (int i = 0; i < left.size(); i++) {
            outputs.add(unionizeSymbol(left.get(i), right.get(i)));
        }
        return outputs;
    }

    private ScopedSymbol unionizeSymbol(Symbol left, Symbol right) {
        return new ScopedSymbol(name, Symbols.pathFromSymbol(left), unionizeTypes(left.valueType(), right.valueType()));
    }

    private DataType<?> unionizeTypes(DataType<?> leftType, DataType<?> rightType) {
        DataType<?> type;
        if (leftType.id() == ObjectType.ID) {
            type = unionizeObjectType((ObjectType) leftType, (ObjectType) rightType);
        } else if (leftType.id() == ArrayType.ID) {
            type = new ArrayType<>(
                unionizeTypes(
                    ((ArrayType<?>) leftType).innerType(),
                    ((ArrayType<?>) rightType).innerType()));
        } else {
            type = leftType.isConvertableTo(rightType, false) ? rightType : leftType;
        }
        return type;
    }

    private ObjectType unionizeObjectType(ObjectType left, ObjectType right) {
        ObjectType.Builder unionizedObjectBuilder = ObjectType.builder();
        for (var e : left.innerTypes().entrySet()) {
            unionizedObjectBuilder.setInnerType(e.getKey(), e.getValue());
        }
        for (var e : right.innerTypes().entrySet()) {
            unionizedObjectBuilder.mergeInnerType(e.getKey(), e.getValue(), this::unionizeTypes);
        }
        return unionizedObjectBuilder.build();
    }
}

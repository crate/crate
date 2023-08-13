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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.jetbrains.annotations.NotNull;

import org.elasticsearch.common.UUIDs;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;
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
        List<Symbol> fieldsFromLeft = left.outputs();
        ArrayList<ScopedSymbol> outputs = new ArrayList<>(fieldsFromLeft.size());
        for (int i = 0; i < fieldsFromLeft.size(); i++) {
            Symbol field = fieldsFromLeft.get(i);
            Symbol rightField = right.outputs().get(i);
            DataType<?> type;
            if (field.valueType().id() == ObjectType.ID) {
                type = new UnionObjectType((ObjectType) field.valueType(), (ObjectType) rightField.valueType());
            } else {
                type = field.valueType().precedes(rightField.valueType())
                    ? field.valueType()
                    : rightField.valueType();
            }
            outputs.add(new ScopedSymbol(name, Symbols.pathFromSymbol(field), type));
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


    // TODO: instead of UnionObjectType, can simply contain duplicate in List<Symbol> and when there are duplicates, check the precedence or return left obj.
    /**
     * This class helps resolve object output's sub-columns of UnionSelect by allowing to search both left and right tables.
     * Except when resolving the sub-columns, this should be identical to 'left' object.
     * Ex)
     *      create table v1 (obj object as (a int))
     *      create table v2 (obj object as (b int))
     *      select obj['a'], obj['b'] from v1 union all select obj from v2
     */
    @VisibleForTesting
    static final class UnionObjectType extends ObjectType {

        private final ObjectType left;
        private final ObjectType right;

        private UnionObjectType(StreamInput in) throws IOException {
            super(in);
            throw new IllegalStateException("UnionObjectType must not be streamed.");
        }

        @VisibleForTesting
        UnionObjectType(ObjectType left, ObjectType right) {
            super(left.innerTypes());
            this.left = left;
            this.right = right;
        }

        /**
         * Searches left object type first then falls back to right object type.
         */
        @Override
        public DataType<?> resolveInnerType(List<String> path) {
            var fromLeft = left.resolveInnerType(path);
            var fromRight = right.resolveInnerType(path);
            return fromLeft.precedes(fromRight) ? fromLeft : fromRight;
        }
    }
}

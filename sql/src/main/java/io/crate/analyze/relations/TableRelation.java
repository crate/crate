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

package io.crate.analyze.relations;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.DynamicReference;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Reference;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class TableRelation implements AnalyzedRelation {

    private static final Predicate<ReferenceInfo> IS_OBJECT_ARRAY = new Predicate<ReferenceInfo>() {
        @Override
        public boolean apply(@Nullable ReferenceInfo input) {
            return input != null
                    && input.type().id() == ArrayType.ID
                    && ((ArrayType)input.type()).innerType().equals(DataTypes.OBJECT);
        }
    };

    private TableInfo tableInfo;
    private List<Field> outputs;

    public TableRelation(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public TableInfo tableInfo() {
        return tableInfo;
    }

    @Override
    public <C, R> R accept(RelationVisitor<C, R> visitor, C context) {
        return visitor.visitTableRelation(this, context);
    }

    @Nullable
    @Override
    public Field getField(ColumnIdent path) {
        return getField(path, false);
    }

    @Override
    public Field getWritableField(ColumnIdent path) throws ColumnUnknownException {
        return getField(path, true);
    }

    private Field getField(ColumnIdent path, boolean forWrite) {
        ReferenceInfo referenceInfo = tableInfo.getReferenceInfo(path);
        if (referenceInfo == null) {
            referenceInfo = tableInfo.indexColumn(path);
            if (referenceInfo == null) {
                DynamicReference dynamic = tableInfo.getDynamic(path, forWrite);
                return new Field(this, path.sqlFqn(), dynamic);
            }
        }
        // TODO: build type correctly as array when the tableInfo is created and remove the conversion here
        if (!path.isColumn() && hasMatchingParent(referenceInfo, IS_OBJECT_ARRAY)) {
            if (DataTypes.isCollectionType(referenceInfo.type())) {
                // TODO: remove this limitation with next type refactoring
                throw new UnsupportedOperationException("cannot query for arrays inside object arrays explicitly");
            }
            // for child fields of object arrays
            // return references of primitive types as array
            referenceInfo = new ReferenceInfo.Builder()
                    .ident(referenceInfo.ident())
                    .columnPolicy(referenceInfo.columnPolicy())
                    .granularity(referenceInfo.granularity())
                    .type(new ArrayType(referenceInfo.type()))
                    .build();
        }
        return new Field(this, path.sqlFqn(), new Reference(referenceInfo));
    }

    @Override
    public List<Field> fields() {
        if (outputs == null) {
            outputs = new ArrayList<>(tableInfo.columns().size());
            for (ReferenceInfo referenceInfo : tableInfo.columns()) {
                if (referenceInfo.type().equals(DataTypes.NOT_SUPPORTED)) {
                    continue;
                }
                ColumnIdent columnIdent = referenceInfo.ident().columnIdent();
                outputs.add(getField(columnIdent));
            }
        }
        return outputs;
    }

    /**
     * return true if the given {@linkplain com.google.common.base.Predicate}
     * returns true for a parent column of this one.
     * returns false if info has no parent column.
     */
    private boolean hasMatchingParent(ReferenceInfo info, Predicate<ReferenceInfo> parentMatchPredicate) {
        ColumnIdent parent = info.ident().columnIdent().getParent();
        while (parent != null) {
            ReferenceInfo parentInfo = tableInfo.getReferenceInfo(parent);
            if (parentMatchPredicate.apply(parentInfo)) {
                return true;
            }
            parent = parent.getParent();
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("table", tableInfo).toString();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableRelation that = (TableRelation) o;

        if (!tableInfo.equals(that.tableInfo)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return tableInfo.hashCode();
    }
}

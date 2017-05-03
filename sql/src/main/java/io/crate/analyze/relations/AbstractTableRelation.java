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

import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Field;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Path;
import io.crate.metadata.Reference;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Predicate;

public abstract class AbstractTableRelation<T extends TableInfo> implements AnalyzedRelation, FieldResolver {

    private static final Predicate<Reference> IS_OBJECT_ARRAY =
        input -> input != null
        && input.valueType().id() == ArrayType.ID
        && ((ArrayType) input.valueType()).innerType().equals(DataTypes.OBJECT);

    protected T tableInfo;
    private List<Field> outputs;
    private Map<Path, Reference> allocatedFields = new HashMap<>();
    private QualifiedName qualifiedName;

    public AbstractTableRelation(T tableInfo) {
        this.tableInfo = tableInfo;
        qualifiedName = new QualifiedName(Arrays.asList(tableInfo.ident().schema(), tableInfo.ident().name()));
    }

    public T tableInfo() {
        return tableInfo;
    }

    @Nullable
    public Field getField(Path path) {
        ColumnIdent ci = toColumnIdent(path);
        Reference reference = tableInfo.getReference(ci);
        if (reference == null) {
            return null;
        }
        reference = checkNestedArray(ci, reference);
        return allocate(ci, reference);

    }

    protected Reference checkNestedArray(ColumnIdent ci, Reference reference) {
        // TODO: build type correctly as array when the tableInfo is created and remove the conversion here
        DataType dataType = null;
        ColumnIdent tmpCI = ci;
        Reference tmpRI = reference;

        while (!tmpCI.isColumn() && hasMatchingParent(tmpRI, IS_OBJECT_ARRAY)) {
            if (DataTypes.isCollectionType(reference.valueType())) {
                // TODO: remove this limitation with next type refactoring
                throw new UnsupportedOperationException("cannot query for arrays inside object arrays explicitly");
            }

            // for child fields of object arrays
            // return references of primitive types as array
            if (dataType == null) {
                dataType = new ArrayType(reference.valueType());
                if (hasNestedObjectReference(tmpRI)) break;
            } else {
                dataType = new ArrayType(dataType);
            }
            tmpCI = tmpCI.getParent();
            tmpRI = tableInfo.getReference(tmpCI);
        }

        if (dataType != null) {
            return new Reference(
                reference.ident(),
                reference.granularity(),
                dataType,
                reference.columnPolicy(),
                reference.indexType(),
                reference.isNullable());
        } else {
            return reference;
        }
    }

    protected static ColumnIdent toColumnIdent(Path path) {
        try {
            return (ColumnIdent) path;
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("TableRelation requires a ColumnIdent as path to get a field");
        }
    }

    protected Field allocate(Path path, Reference reference) {
        allocatedFields.put(path, reference);
        return new Field(this, path, reference.valueType());
    }

    @Override
    public List<Field> fields() {
        if (outputs == null) {
            outputs = new ArrayList<>(tableInfo.columns().size());
            for (Reference reference : tableInfo.columns()) {
                if (reference.valueType().equals(DataTypes.NOT_SUPPORTED)) {
                    continue;
                }
                ColumnIdent columnIdent = reference.ident().columnIdent();
                outputs.add(getField(columnIdent));
            }
        }
        return outputs;
    }

    @Override
    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    /**
     * return true if the given {@linkplain com.google.common.base.Predicate}
     * returns true for a parent column of this one.
     * returns false if info has no parent column.
     */
    private boolean hasMatchingParent(Reference info, Predicate<Reference> parentMatchPredicate) {
        ColumnIdent parent = info.ident().columnIdent().getParent();
        while (parent != null) {
            Reference parentInfo = tableInfo.getReference(parent);
            if (parentMatchPredicate.test(parentInfo)) {
                return true;
            }
            parent = parent.getParent();
        }
        return false;
    }

    private boolean hasNestedObjectReference(Reference info) {
        ColumnIdent parent = info.ident().columnIdent().getParent();
        if (parent != null) {
            Reference parentRef = tableInfo.getReference(parent);
            if (parentRef.valueType().id() == ObjectType.ID) {
                return hasMatchingParent(parentRef, IS_OBJECT_ARRAY);
            }
        }
        return false;
    }

    @Override
    public String toString() {
        String tableName = tableInfo.ident().toString();
        String qualifiedName = this.qualifiedName.toString();
        // to be able to distinguish tables in a self-joins
        if (tableName.equals(qualifiedName)) {
            return getClass().getSimpleName() + '{' + tableName + '}';
        }
        return getClass().getSimpleName() + '{' + tableName + " AS " + qualifiedName + '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractTableRelation that = (AbstractTableRelation) o;

        if (!tableInfo.equals(that.tableInfo)) return false;
        if (!qualifiedName.equals(that.qualifiedName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tableInfo.hashCode();
        result = 31 * result + qualifiedName.hashCode();
        return result;
    }

    @Override
    @Nullable
    public Reference resolveField(Field field) {
        if (field.relation().equals(this)) {
            return allocatedFields.get(field.path());
        }
        return null;
    }

    public void validateOrderBy(Optional<OrderBy> orderBy) {
    }
}

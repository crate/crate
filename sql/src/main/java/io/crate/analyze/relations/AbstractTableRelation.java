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

import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public abstract class AbstractTableRelation<T extends TableInfo> implements AnalyzedRelation, FieldResolver {

    private static final Predicate<Reference> IS_OBJECT_ARRAY =
        input -> input != null
        && input.valueType().id() == ArrayType.ID
        && ((ArrayType) input.valueType()).innerType().id() == ObjectType.ID;

    protected final T tableInfo;
    private final Map<ColumnIdent, Reference> allocatedFields = new HashMap<>();
    private final List<Symbol> outputs;
    private List<Field> fields;
    private final QualifiedName qualifiedName;

    public AbstractTableRelation(T tableInfo) {
        this(tableInfo, new QualifiedName(Arrays.asList(tableInfo.ident().schema(), tableInfo.ident().name())));
    }

    public AbstractTableRelation(T tableInfo, QualifiedName qualifiedName) {
        this.tableInfo = tableInfo;
        this.qualifiedName = qualifiedName;
        outputs = new ArrayList<>(tableInfo.columns());
        fields = new ArrayList<>(tableInfo.columns().size());
        for (Reference reference : tableInfo.columns()) {
            if (reference.valueType().equals(DataTypes.NOT_SUPPORTED)) {
                continue;
            }
            fields.add(new Field(this, reference.column(), reference));
            // Allocate it so it can be resolved by resolveField()
            allocatedFields.put(reference.column(), reference);
        }
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    public T tableInfo() {
        return tableInfo;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public WhereClause where() {
        return WhereClause.MATCH_ALL;
    }

    @Override
    public List<Symbol> groupBy() {
        return List.of();
    }

    @Nullable
    @Override
    public HavingClause having() {
        return null;
    }

    @Nullable
    @Override
    public OrderBy orderBy() {
        return null;
    }

    @Nullable
    @Override
    public Symbol limit() {
        return null;
    }

    @Nullable
    @Override
    public Symbol offset() {
        return null;
    }

    @Override
    public boolean hasAggregates() {
        return false;
    }

    @Nullable
    public Field getField(ColumnIdent path) {
        Reference reference = tableInfo.getReference(path);
        if (reference == null) {
            return null;
        }
        return allocate(path, makeArrayIfContainedInObjectArray(reference));
    }

    /**
     * Changes the type of a reference to be an array if the reference is the child of an object array
     *
     * <pre>
     * {@code
     *      CREATE TABLE t (addresses array(object as (street string)))
     *      -> `street` isolated within a single object is a string
     *      -> Accessed via `addresses['street']` (E.g. in a SELECT) it becomes an ARRAY because `addresses` is
     * }
     * </pre>
     */
    Reference makeArrayIfContainedInObjectArray(Reference reference) {
        Reference rootRef = reference;
        int numArrayDimensions = 0;
        while ((!rootRef.column().isTopLevel())) {
            rootRef = tableInfo.getReference(rootRef.column().getParent());
            assert rootRef != null : "The parent column of a nested object column must always exist";
            if (IS_OBJECT_ARRAY.test(rootRef)) {
                numArrayDimensions++;
            }
        }
        return numArrayDimensions == 0
            ? reference
            : new Reference(
                reference.ident(),
                reference.granularity(),
                makeArray(reference.valueType(), numArrayDimensions),
                reference.columnPolicy(),
                reference.indexType(),
                reference.isNullable(),
                reference.isColumnStoreDisabled(),
                reference.position(),
                reference.defaultExpression()
            );
    }

    private static DataType makeArray(DataType valueType, int numArrayDimensions) {
        DataType arrayType = valueType;
        for (int i = 0; i < numArrayDimensions; i++) {
            arrayType = new ArrayType(arrayType);
        }
        return arrayType;
    }

    protected Field allocate(ColumnIdent path, Reference reference) {
        allocatedFields.put(path, reference);
        // Add column to the outputs, so outer relations (join, subselect) can resolve it.
        // Required for resolving subscript cols without any expression mapping (No QueriedTable on top of this relation).
        if (outputs.contains(reference) == false) {
            outputs.add(reference);
        }
        Field f = new Field(this, path, reference);

        // Subscript fields aren't initially loaded as there could be many, add them now as requested.
        if (path.isTopLevel() == false && fields.contains(f) == false) {
            fields.add(f);
        }
        return f;
    }

    @Override
    public List<Field> fields() {
        return fields;
    }

    @Override
    public QualifiedName getQualifiedName() {
        return qualifiedName;
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
}

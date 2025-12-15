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

package io.crate.metadata;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.doc.SysColumns;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.Expression;
import io.crate.types.DataType;

public interface UnscopedRef extends Symbol {

    public static final Comparator<ScopedRef> CMP_BY_POSITION_THEN_NAME = Comparator
        .comparingInt(ScopedRef::position)
        .thenComparing(r -> r.column().fqn());

    public static int indexOf(Iterable<? extends UnscopedRef> refs, ColumnIdent column) {
        int i = 0;
        for (UnscopedRef ref : refs) {
            if (ref.column().equals(column)) {
                return i;
            }
            i++;
        }
        return -1;
    }


    ColumnIdent column();

    @Override
    default ColumnIdent toColumn() {
        return column();
    }

    @Override
    default ColumnDefinition<Expression> toColumnDefinition() {
        return new ColumnDefinition<>(
            toColumn().sqlFqn(), // allow ObjectTypes to return col name in subscript notation
            valueType().toColumnType(null),
            List.of()
        );
    }

    IndexType indexType();

    boolean isNullable();

    RowGranularity granularity();

    int position();

    /**
     * This is used as a column name in the source
     */
    long oid();

    boolean isDropped();

    boolean hasDocValues();

    @Nullable
    Symbol defaultExpression();

    boolean isGenerated();

    ScopedRef withColumn(ColumnIdent column);

    ScopedRef withOidAndPosition(LongSupplier acquireOid, IntSupplier acquirePosition);

    ScopedRef withDropped(boolean dropped);

    ScopedRef withValueType(DataType<?> type);

    /**
     * Return the identifier of this column used inside the storage engine
     */
    default String storageIdent() {
        long oid = oid();
        if (oid == Metadata.COLUMN_OID_UNASSIGNED) {
            ColumnIdent column = column();
            if (column.isRoot() == false && column.name().equals(SysColumns.Names.DOC)) {
                column = column.shiftRight();
            }
            return column.fqn();
        } else {
            return Long.toString(oid);
        }
    }

    /**
     * Return the identifier of this column used inside the storage engine.
     * Compared to {@link #storageIdent()}, this will return the columns leaf name instead of the FQN
     * if no OID is assigned.
     */
    default String storageIdentLeafName() {
        return oid() == Metadata.COLUMN_OID_UNASSIGNED ? column().leafName() : Long.toString(oid());
    }

    /**
     * Creates the {@link IndexMetadata} mapping representation of the Column.
     * <p>
     * Note that for object types it does _NOT_ include the inner columns.
     * </p>
     *
     * @param position position to use in the mapping
     */
    Map<String, Object> toMapping(int position);

}

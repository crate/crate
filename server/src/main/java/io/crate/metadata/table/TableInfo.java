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

package io.crate.metadata.table;

import static io.crate.types.ArrayType.makeArray;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import org.elasticsearch.cluster.ClusterState;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public interface TableInfo extends RelationInfo {

    Predicate<DataType<?>> IS_OBJECT_ARRAY =
        type -> type instanceof ArrayType && ((ArrayType<?>) type).innerType().id() == ObjectType.ID;

    /**
     * returns information about a column with the given ident.
     * returns null if this table contains no such column.
     */
    @Nullable
    Reference getReference(ColumnIdent columnIdent);

    /**
     * This is like {@link #getReference(ColumnIdent)},
     * except that the type is adjusted via {@link #getReadType(ColumnIdent)}
     */
    @Nullable
    default Reference getReadReference(ColumnIdent columnIdent) {
        Reference ref = getReference(columnIdent);
        if (ref == null) {
            return null;
        }
        DataType<?> readType = getReadType(columnIdent);
        if (readType.equals(ref.valueType())) {
            return ref;
        } else {
            return new SimpleReference(
                ref.ident(),
                ref.granularity(),
                readType,
                ref.columnPolicy(),
                ref.indexType(),
                ref.isNullable(),
                ref.hasDocValues(),
                ref.position(),
                ref.oid(),
                ref.isDropped(),
                ref.defaultExpression()
            );
        }
    }

    /**
     * Get a Iterable over the parents of a column.
     * Elements can be null if the column itself is unknown.
     **/
    default Iterable<Reference> getParents(ColumnIdent column) {
        if (column.isRoot()) {
            return Collections.emptyList();
        }
        return () -> new Iterator<Reference>() {

            ColumnIdent current = column;

            @Override
            public boolean hasNext() {
                return !current.isRoot();
            }

            @Override
            public Reference next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Column has no more parent");
                }
                current = current.getParent();
                return getReference(current);
            }
        };
    }

    /**
     * Returns the type of the column for reading.
     * <p>
     *      Columns can have two different types. Given the schema:
     * </p>
     *
     * <pre>
     * {@code
     *  payloads ARRAY(
     *      OBJECT(STRICT) AS (
     *          x INTEGER
     *      )
     *  )
     * }
     * </pre>
     *
     * Reads on `payloads['x']` return a `array(integer)`,
     * but users would insert something like `[{x: 10}, {x: 20}]` where `x` is an integer.
     *
     * @return the type of the column; morphed to array if it is the child of an array object.
     *         UNDEFINED if the column does not exist.
     */
    default DataType<?> getReadType(ColumnIdent column) {
        Reference ref = getReference(column);
        if (ref == null) {
            return DataTypes.UNDEFINED;
        }
        int arrayDimensions = 0;
        for (var parent : getParents(column)) {
            if (IS_OBJECT_ARRAY.test(parent.valueType())) {
                arrayDimensions++;
            }
        }
        return makeArray(ref.valueType(), arrayDimensions);
    }

    /**
     * Retrieve the routing for the table
     *
     * <p>
     *   Multiple calls to this method return the same routing as long as the same arguments are provided.
     * <p>
     */
    Routing getRouting(ClusterState state,
                       RoutingProvider routingProvider,
                       WhereClause whereClause,
                       RoutingProvider.ShardSelection shardSelection,
                       CoordinatorSessionSettings sessionSettings);

}

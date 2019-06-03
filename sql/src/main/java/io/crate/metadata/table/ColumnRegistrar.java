/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.table;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnRegistrar {
    private final ImmutableSortedMap.Builder<ColumnIdent, Reference> infosBuilder;
    private final ImmutableSortedSet.Builder<Reference> columnsBuilder;

    private final RelationName relationName;
    private final RowGranularity rowGranularity;

    private int position = 1;

    public ColumnRegistrar(RelationName relationName, RowGranularity rowGranularity) {
        this.relationName = relationName;
        this.rowGranularity = rowGranularity;
        this.infosBuilder = ImmutableSortedMap.naturalOrder();
        this.columnsBuilder = ImmutableSortedSet.orderedBy(Reference.COMPARE_BY_COLUMN_IDENT);
    }

    public ColumnRegistrar register(String column, DataType type) {
        return register(new ColumnIdent(column), type);
    }

    public ColumnRegistrar register(ColumnIdent column, DataType type) {
        return register(column, type, true);
    }

    public ColumnRegistrar register(ColumnIdent column, DataType type, boolean nullable) {
        Reference ref = new Reference(
            new ReferenceIdent(relationName, column),
            rowGranularity,
            type,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            nullable,
            position,
            null
        );
        position++;
        if (ref.column().isTopLevel()) {
            columnsBuilder.add(ref);
        }
        infosBuilder.put(ref.column(), ref);
        registerPossibleObjectInnerTypes(column.name(), column.path(), type);
        return this;
    }

    private void registerPossibleObjectInnerTypes(String topLevelName, List<String> path, DataType dataType) {
        if (DataTypes.isCollectionType(dataType)) {
            dataType = ((CollectionType) dataType).innerType();
        }
        if (dataType.id() != ObjectType.ID) {
            return;
        }
        Map<String, DataType> innerTypes = ((ObjectType) dataType).innerTypes();
        int pos = 0;
        for (Map.Entry<String, DataType> entry : innerTypes.entrySet()) {
            List<String> subPath = new ArrayList<>(path);
            subPath.add(entry.getKey());
            ColumnIdent ci = new ColumnIdent(topLevelName, subPath);
            DataType innerType = entry.getValue();
            Reference ref = new Reference(
                new ReferenceIdent(relationName, ci),
                rowGranularity,
                innerType,
                ColumnPolicy.STRICT,
                Reference.IndexType.NOT_ANALYZED,
                true,
                pos,
                null
            );
            pos++;
            infosBuilder.put(ref.column(), ref);
            registerPossibleObjectInnerTypes(ci.name(), ci.path(), innerType);
        }
    }

    public ColumnRegistrar putInfoOnly(ColumnIdent columnIdent, Reference reference) {
        infosBuilder.put(columnIdent, reference);
        return this;
    }

    public Map<ColumnIdent, Reference> infos() {
        return infosBuilder.build();
    }

    public Set<Reference> columns() {
        return columnsBuilder.build();
    }
}

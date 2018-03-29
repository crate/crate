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
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnRegistrar {
    private final ImmutableSortedMap.Builder<ColumnIdent, Reference> infosBuilder;
    private final ImmutableSortedSet.Builder<Reference> columnsBuilder;

    private final RelationName relationName;
    private final RowGranularity rowGranularity;

    public ColumnRegistrar(RelationName relationName, RowGranularity rowGranularity) {
        this.relationName = relationName;
        this.rowGranularity = rowGranularity;
        this.infosBuilder = ImmutableSortedMap.naturalOrder();
        this.columnsBuilder = ImmutableSortedSet.orderedBy(Reference.COMPARE_BY_COLUMN_IDENT);
    }

    public ColumnRegistrar register(String column, DataType type, @Nullable List<String> path) {
        return register(new ColumnIdent(column, path), type);
    }

    public ColumnRegistrar register(ColumnIdent column, DataType type) {
        Reference ref = new Reference(new ReferenceIdent(relationName, column), rowGranularity, type);
        if (ref.column().isTopLevel()) {
            columnsBuilder.add(ref);
        }
        infosBuilder.put(ref.column(), ref);
        return this;
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

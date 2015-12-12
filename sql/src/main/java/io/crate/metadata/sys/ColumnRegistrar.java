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

package io.crate.metadata.sys;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import io.crate.metadata.*;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ColumnRegistrar {
    private final ImmutableSortedMap.Builder<ColumnIdent, ReferenceInfo> infosBuilder;
    private final ImmutableSortedSet.Builder<ReferenceInfo> columnsBuilder;

    private final TableIdent tableIdent;
    private final RowGranularity rowGranularity;

    ColumnRegistrar(TableIdent tableIdent,
                    RowGranularity rowGranularity) {
        this.tableIdent = tableIdent;
        this.rowGranularity = rowGranularity;
        this.infosBuilder = ImmutableSortedMap.naturalOrder();
        this.columnsBuilder = ImmutableSortedSet.orderedBy(ReferenceInfo.COMPARE_BY_COLUMN_IDENT);
    }

    public ColumnRegistrar register(String column, DataType type, @Nullable List<String> path) {
        return register(new ColumnIdent(column, path), type);
    }

    public ColumnRegistrar register(ColumnIdent column, DataType type) {
        ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(tableIdent, column), rowGranularity, type);
        if (info.ident().isColumn()) {
            columnsBuilder.add(info);
        }
        infosBuilder.put(info.ident().columnIdent(), info);
        return this;
    }

    public ColumnRegistrar putInfoOnly(ColumnIdent columnIdent, ReferenceInfo referenceInfo) {
        infosBuilder.put(columnIdent, referenceInfo);
        return this;
    }

    public Map<ColumnIdent, ReferenceInfo> infos() {
        return infosBuilder.build();
    }

    public Set<ReferenceInfo> columns() {
        return columnsBuilder.build();
    }
}

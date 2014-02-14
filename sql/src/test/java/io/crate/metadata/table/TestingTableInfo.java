/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import org.cratedb.DataType;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TestingTableInfo implements TableInfo {

    private final Routing routing;

    public static Builder builder(TableIdent ident, RowGranularity granularity, Routing routing) {
        return new Builder(ident, granularity, routing);
    }

    public static class Builder {

        private final ImmutableList.Builder<ReferenceInfo> columns = ImmutableList.builder();
        private final ImmutableMap.Builder<ColumnIdent, ReferenceInfo> references = ImmutableMap.builder();
        private final ImmutableList.Builder<String> primaryKey = ImmutableList.builder();

        private final RowGranularity granularity;
        private final TableIdent ident;
        private final Routing routing;

        public Builder(TableIdent ident, RowGranularity granularity, Routing routing) {
            this.granularity = granularity;
            this.routing = routing;
            this.ident = ident;
        }

        public Builder add(String column, DataType type, List<String> path) {
            ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(ident, column, path), granularity, type);
            if (info.ident().isColumn()) {
                columns.add(info);
            }
            references.put(info.ident().columnIdent(), info);
            return this;
        }

        public Builder addPrimaryKey(String column) {
            primaryKey.add(column);
            return this;
        }

        public TableInfo build() {
            return new TestingTableInfo(
                    columns.build(), references.build(), ident, granularity, routing, primaryKey.build());
        }
    }


    private final List<ReferenceInfo> columns;
    private final Map<ColumnIdent, ReferenceInfo> references;
    private final TableIdent ident;
    private final RowGranularity granularity;
    private final List<String> primaryKey;

    public TestingTableInfo(List<ReferenceInfo> columns, Map<ColumnIdent, ReferenceInfo> references,
                            TableIdent ident, RowGranularity granularity,
                            Routing routing,
                            List<String> primaryKey) {
        this.columns = columns;
        this.references = references;
        this.ident = ident;
        this.granularity = granularity;
        this.routing = routing;
        this.primaryKey = primaryKey;
    }

    @Override
    public ReferenceInfo getColumnInfo(ColumnIdent columnIdent) {
        return references.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }

    @Override
    public RowGranularity rowGranularity() {
        return granularity;
    }

    @Override
    public TableIdent ident() {
        return ident;
    }

    @Override
    public Routing getRouting(Function whereClause) {
        return routing;
    }

    @Override
    public List<String> primaryKey() {
        return primaryKey;
    }

    @Override
    public String clusteredBy() {
        return null;
    }
}

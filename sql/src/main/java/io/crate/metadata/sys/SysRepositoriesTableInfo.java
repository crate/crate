/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.sys;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;

public class SysRepositoriesTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "repositories");
    private final ClusterService clusterService;

    public static class Columns {
        public static final ColumnIdent NAME = new ColumnIdent("name");
        public static final ColumnIdent TYPE = new ColumnIdent("type");
        public static final ColumnIdent SETTINGS = new ColumnIdent("settings");
    }

    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(Columns.NAME);
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    public SysRepositoriesTableInfo(ClusterService clusterService) {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
            .register(Columns.NAME, DataTypes.STRING)
            .register(Columns.TYPE, DataTypes.STRING)
            .register(Columns.SETTINGS, DataTypes.OBJECT), PRIMARY_KEY);
        this.clusterService = clusterService;
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        return Routing.forRandomMasterOrDataNode(IDENT, clusterService);
    }
}

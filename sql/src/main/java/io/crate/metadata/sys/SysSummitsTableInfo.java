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

import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SessionContext;
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
import java.util.List;

public class SysSummitsTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "summits");
    private static final List<ColumnIdent> PRIMARY_KEYS = ImmutableList.of(Columns.MOUNTAIN);
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    private final ClusterService clusterService;

    public static class Columns {
        public static final ColumnIdent MOUNTAIN = new ColumnIdent("mountain");
        public static final ColumnIdent HEIGHT = new ColumnIdent("height");
        public static final ColumnIdent PROMINENCE = new ColumnIdent("prominence");
        public static final ColumnIdent COORDINATES = new ColumnIdent("coordinates");
        public static final ColumnIdent RANGE = new ColumnIdent("range");
        public static final ColumnIdent CLASSIFICATION = new ColumnIdent("classification");
        public static final ColumnIdent REGION = new ColumnIdent("region");
        public static final ColumnIdent COUNTRY = new ColumnIdent("country");
        public static final ColumnIdent FIRST_ASCENT = new ColumnIdent("first_ascent");
    }

    SysSummitsTableInfo(ClusterService clusterService) {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
            .register(Columns.MOUNTAIN, DataTypes.STRING)
            .register(Columns.HEIGHT, DataTypes.INTEGER)
            .register(Columns.PROMINENCE, DataTypes.INTEGER)
            .register(Columns.COORDINATES, DataTypes.GEO_POINT)
            .register(Columns.RANGE, DataTypes.STRING)
            .register(Columns.CLASSIFICATION, DataTypes.STRING)
            .register(Columns.REGION, DataTypes.STRING)
            .register(Columns.COUNTRY, DataTypes.STRING)
            .register(Columns.FIRST_ASCENT, DataTypes.INTEGER), PRIMARY_KEYS);
        this.clusterService = clusterService;
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference, SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(IDENT, clusterService.localNode().getId());
    }
}

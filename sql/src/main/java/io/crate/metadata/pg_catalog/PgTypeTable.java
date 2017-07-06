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

package io.crate.metadata.pg_catalog;

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
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Collections;

public class PgTypeTable extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(PgCatalogSchemaInfo.NAME, "pg_type");
    private final ClusterService clusterService;

    static class Columns {
        static final ColumnIdent OID = new ColumnIdent("oid");
        static final ColumnIdent TYPNAME = new ColumnIdent("typname");
        static final ColumnIdent TYPDELIM = new ColumnIdent("typdelim");
        static final ColumnIdent TYPELEM = new ColumnIdent("typelem");
    }

    @Inject
    public PgTypeTable(ClusterService clusterService) {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
                .register("oid", DataTypes.INTEGER, null)
                .register("typname", DataTypes.STRING, null)
                .register("typdelim", DataTypes.STRING, null)
                .register("typelem", DataTypes.INTEGER, null),
            Collections.<ColumnIdent>emptyList());
        this.clusterService = clusterService;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference, SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(IDENT, clusterService.localNode().getId());
    }
}

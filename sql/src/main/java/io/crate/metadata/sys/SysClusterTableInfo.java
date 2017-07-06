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

package io.crate.metadata.sys;

import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.operation.reference.sys.cluster.ClusterLoggingOverridesExpression;
import io.crate.operation.reference.sys.cluster.ClusterSettingsExpression;
import io.crate.settings.CrateSetting;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class SysClusterTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "cluster");

    private final ClusterService clusterService;

    SysClusterTableInfo(ClusterService clusterService) {
        super(IDENT, buildColumnRegistrar(), Collections.emptyList());
        this.clusterService = clusterService;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference, SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(IDENT, clusterService.localNode().getId());
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.CLUSTER;
    }

    private static ColumnRegistrar buildColumnRegistrar() {
        ColumnRegistrar columnRegistrar = new ColumnRegistrar(IDENT, RowGranularity.CLUSTER)
            .register("id", DataTypes.STRING, null)
            .register("name", DataTypes.STRING, null)
            .register("master_node", DataTypes.STRING, null)
            .register(ClusterSettingsExpression.NAME, DataTypes.OBJECT, null)
            // custom registration of logger expressions
            .register(ClusterSettingsExpression.NAME, new ArrayType(DataTypes.OBJECT), ImmutableList.of(
                ClusterLoggingOverridesExpression.NAME))
            .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(
                ClusterLoggingOverridesExpression.NAME,
                ClusterLoggingOverridesExpression.ClusterLoggingOverridesChildExpression.NAME))
            .register(ClusterSettingsExpression.NAME, DataTypes.STRING, ImmutableList.of(
                ClusterLoggingOverridesExpression.NAME,
                ClusterLoggingOverridesExpression.ClusterLoggingOverridesChildExpression.LEVEL));

        // register all exposed crate and elasticsearch settings
        for (CrateSetting<?> crateSetting : CrateSettings.BUILT_IN_SETTINGS) {
            List<String> namePath = crateSetting.path();
            createParentColumnIfMissing(columnRegistrar, namePath);
            // don't register empty groups
            if (namePath.get(namePath.size()-1).isEmpty() == false) {
                columnRegistrar.register(ClusterSettingsExpression.NAME, crateSetting.dataType(), namePath);
            }
        }

        return columnRegistrar;
    }

    private static void createParentColumnIfMissing(ColumnRegistrar columnRegistrar, List<String> nameParts) {
        for (int i = 1; i < nameParts.size(); i++) {
            ColumnIdent columnIdent = new ColumnIdent(ClusterSettingsExpression.NAME, nameParts.subList(0, i));
            if (columnRegistrar.infos().get(columnIdent) == null) {
                columnRegistrar.register(columnIdent, DataTypes.OBJECT);
            }
        }
    }


}

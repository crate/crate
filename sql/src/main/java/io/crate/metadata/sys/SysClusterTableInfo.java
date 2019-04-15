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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.expression.reference.sys.cluster.ClusterLoggingOverridesExpression;
import io.crate.expression.reference.sys.cluster.ClusterSettingsExpression;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.settings.CrateSetting;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.crate.expression.reference.sys.cluster.ClusterLicenseExpression.EXPIRY_DATE;
import static io.crate.expression.reference.sys.cluster.ClusterLicenseExpression.ISSUED_TO;
import static io.crate.expression.reference.sys.cluster.ClusterLicenseExpression.MAX_NODES;

public class SysClusterTableInfo extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "cluster");

    SysClusterTableInfo() {
        super(IDENT, buildColumnRegistrar(), Collections.emptyList());
    }

    @Override
    public Routing getRouting(ClusterState clusterState,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(IDENT, clusterState.getNodes().getLocalNodeId());
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.CLUSTER;
    }

    private static ColumnRegistrar buildColumnRegistrar() {
        return new ColumnRegistrar(IDENT, RowGranularity.CLUSTER)
            .register("id", DataTypes.STRING)
            .register("name", DataTypes.STRING)
            .register("master_node", DataTypes.STRING)
            .register("license",
                ObjectType.builder()
                    .setInnerType(EXPIRY_DATE, DataTypes.TIMESTAMPZ)
                    .setInnerType(ISSUED_TO, DataTypes.STRING)
                    .setInnerType(MAX_NODES, DataTypes.INTEGER)
                    .build())
            .register(ClusterSettingsExpression.NAME, buildSettingsObjectType());
    }

    private static ObjectType buildSettingsObjectType() {
        ObjectType.Builder settingTypeBuilder = ObjectType.builder()
            .setInnerType(ClusterLoggingOverridesExpression.NAME, new ArrayType(
                ObjectType.builder()
                    .setInnerType(ClusterLoggingOverridesExpression.ClusterLoggingOverridesChildExpression.NAME, DataTypes.STRING)
                    .setInnerType(ClusterLoggingOverridesExpression.ClusterLoggingOverridesChildExpression.LEVEL, DataTypes.STRING)
                    .build()));

        // register all exposed crate and elasticsearch settings
        Map<String, CrateSetting<?>> settingsMap = new HashMap<>();
        Settings.Builder settingsBuilder = Settings.builder();
        for (CrateSetting<?> crateSetting : CrateSettings.BUILT_IN_SETTINGS) {
            settingsBuilder.put(crateSetting.getKey(), crateSetting.getDefault().toString());
            settingsMap.put(crateSetting.getKey(), crateSetting);
        }

        Map<String, Object> structuredSettingsMap = settingsBuilder.build().getAsStructuredMap();
        for (Map.Entry<String, Object> entry : structuredSettingsMap.entrySet()) {
            buildObjectType(settingTypeBuilder, entry.getKey(), entry.getValue(), Collections.emptyList(), settingsMap::get);
        }

        return settingTypeBuilder.build();
    }

    private static void buildObjectType(ObjectType.Builder builder,
                                        String name,
                                        Object val,
                                        List<String> path,
                                        Function<String, CrateSetting> resolver) {
        List<String> newPath = new ArrayList<>(path);
        newPath.add(name);
        String fqnName = String.join(".", newPath);
        if (val instanceof Map) {
            ObjectType.Builder innerBuilder = ObjectType.builder();
            //noinspection unchecked
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) val).entrySet()) {
                buildObjectType(innerBuilder, entry.getKey(), entry.getValue(), newPath, resolver);
            }
            builder.setInnerType(name, innerBuilder.build());
        } else {
            CrateSetting crateSetting = resolver.apply(fqnName);
            builder.setInnerType(name, crateSetting.dataType());
        }
    }

}

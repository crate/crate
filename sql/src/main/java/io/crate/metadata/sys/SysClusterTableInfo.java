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
import io.crate.analyze.WhereClause;
import io.crate.metadata.*;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.settings.Setting;
import io.crate.planner.RowGranularity;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;

import java.util.*;

public class SysClusterTableInfo extends SysTableInfo {

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "cluster");
    public static final Routing ROUTING = new Routing(
            MapBuilder.<String, Map<String, Set<Integer>>>newMapBuilder().put(
                    null,
                    MapBuilder.<String, Set<Integer>>newMapBuilder().put(IDENT.fqn(), null).map()
            ).map()
    );
    private static final String[] PARTITIONS = new String[]{IDENT.name()};

    public static final Map<ColumnIdent, ReferenceInfo> INFOS = new LinkedHashMap<>();
    private static final LinkedHashSet<ReferenceInfo> columns = new LinkedHashSet<>();

    static {
        register("id", DataTypes.STRING, null);
        register("name", DataTypes.STRING, null);
        register("master_node", DataTypes.STRING, null);
        register("settings", DataTypes.OBJECT, null);

        register("settings", DataTypes.INTEGER, ImmutableList.of(CrateSettings.JOBS_LOG_SIZE.name()));
        register("settings", DataTypes.INTEGER, ImmutableList.of(CrateSettings.OPERATIONS_LOG_SIZE.name()));
        register("settings", DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.COLLECT_STATS.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.GRACEFUL_STOP.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.GRACEFUL_STOP.name(),
                CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.name()));
        register("settings", DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.GRACEFUL_STOP.name(),
                CrateSettings.GRACEFUL_STOP_REALLOCATE.name()));
        register("settings", DataTypes.LONG, ImmutableList.of(CrateSettings.GRACEFUL_STOP.name(),
                CrateSettings.GRACEFUL_STOP_TIMEOUT.name()));
        register("settings", DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.GRACEFUL_STOP.name(),
                CrateSettings.GRACEFUL_STOP_FORCE.name()));
        register("settings", DataTypes.BOOLEAN, ImmutableList.of(CrateSettings.GRACEFUL_STOP.name(),
                CrateSettings.GRACEFUL_STOP_IS_DEFAULT.name()));

        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.ROUTING.name()));
        register("settings", DataTypes.OBJECT, ImmutableList.of(CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name()));
        register("settings", DataTypes.STRING, ImmutableList.of(CrateSettings.ROUTING.name(),
                CrateSettings.ROUTING_ALLOCATION.name(),
                CrateSettings.ROUTING_ALLOCATION_ENABLE.name()));
    }

    @Inject
    protected SysClusterTableInfo(ClusterService clusterService) {
        super(clusterService);
    }

    private static ReferenceInfo register(String column, DataType type, List<String> path) {
        ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(IDENT, column, path), RowGranularity.CLUSTER, type);
        if (info.ident().isColumn()) {
            columns.add(info);
        }
        INFOS.put(info.ident().columnIdent(), info);
        return info;
    }

    @Override
    public ReferenceInfo getColumnInfo(ColumnIdent columnIdent) {
        return INFOS.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }

    @Override
    public Routing getRouting(WhereClause whereClause) {
        return ROUTING;
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return ImmutableList.of();
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.CLUSTER;
    }

    @Override
    public TableIdent ident() {
        return IDENT;
    }

    @Override
    public String[] concreteIndices() {
        return PARTITIONS;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return INFOS.values().iterator();
    }
}

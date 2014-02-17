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

package org.cratedb.plugin;

import io.crate.executor.transport.TransportExecutorModule;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.doc.MetaDataDocModule;
import io.crate.metadata.shard.MetaDataShardModule;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.operations.collect.CollectOperationModule;
import io.crate.operator.operations.collect.CollectShardModule;
import io.crate.operator.operator.OperatorModule;
import io.crate.operator.predicate.PredicateModule;
import io.crate.operator.reference.sys.cluster.SysClusterExpressionModule;
import io.crate.operator.reference.sys.node.SysNodeExpressionModule;
import io.crate.operator.reference.sys.shard.SysShardExpressionModule;
import io.crate.operator.scalar.ScalarFunctionModule;
import io.crate.planner.PlanModule;
import org.cratedb.module.SQLModule;
import org.cratedb.rest.action.RestSQLAction;
import org.cratedb.service.InformationSchemaService;
import org.cratedb.service.SQLService;
import org.cratedb.service.StatsService;
import org.cratedb.sql.facet.SQLFacetParser;
import org.elasticsearch.cluster.settings.ClusterDynamicSettingsModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.search.facet.FacetModule;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;

public class SQLPlugin extends AbstractPlugin {

    private final Settings settings;

    public SQLPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Settings additionalSettings() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();

        // Set default analyzer
        settingsBuilder.put("index.analysis.analyzer.default.type", "keyword");

        // do not map source on GetRequests
        // evaluated in elasticsearch ShardGetService.innerGet
        settingsBuilder.put("index.mapper.map_source", false);
        return settingsBuilder.build();
    }

    public String name() {
        return "sql";
    }

    public String description() {
        return "plugin that adds an /_sql endpoint to query crate with sql";
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        if (!settings.getAsBoolean("node.client", false)) {
            Collection<Class<? extends LifecycleComponent>> services = newArrayList();
            services.add(SQLService.class);
            services.add(InformationSchemaService.class);
            services.add(StatsService.class);
            return services;
        }

        return super.services();
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        if (!settings.getAsBoolean("node.client", false)) {
            modules.add(SQLModule.class);

            modules.add(TransportExecutorModule.class);
            modules.add(CollectOperationModule.class);
            modules.add(MetaDataModule.class);
            modules.add(MetaDataSysModule.class);
            modules.add(MetaDataDocModule.class);
            modules.add(OperatorModule.class);
            modules.add(PredicateModule.class);
            modules.add(SysClusterExpressionModule.class);
            modules.add(SysNodeExpressionModule.class);
            modules.add(AggregationImplModule.class);
            modules.add(ScalarFunctionModule.class);
            modules.add(PlanModule.class);
        }
        return modules;
    }

    @Override
    public Collection<Class<? extends Module>> shardModules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        if (!settings.getAsBoolean("node.client", false)) {
            modules.add(MetaDataShardModule.class);
            modules.add(SysShardExpressionModule.class);
            modules.add(CollectShardModule.class);
        }
        return modules;
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(RestSQLAction.class);
    }

    public void onModule(FacetModule facetModule) {
        facetModule.addFacetProcessor(SQLFacetParser.class);
    }

    public void onModule(ClusterDynamicSettingsModule clusterDynamicSettingsModule) {
        // add our dynamic cluster settings
        clusterDynamicSettingsModule.addDynamicSettings(SQLService.CUSTOM_ANALYSIS_SETTINGS_PREFIX + "*");
    }

}

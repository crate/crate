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

package io.crate.plugin;

import com.google.common.collect.ImmutableList;
import io.crate.Constants;
import io.crate.analyze.repositories.RepositorySettingsModule;
import io.crate.breaker.CircuitBreakerModule;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.cluster.gracefulstop.DecommissionAllocationDecider;
import io.crate.cluster.gracefulstop.DecommissioningService;
import io.crate.executor.transport.TransportExecutorModule;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobModule;
import io.crate.jobs.transport.NodeDisconnectJobMonitorService;
import io.crate.lucene.CrateIndexModule;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.MetaDataBlobModule;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.metadata.pg_catalog.PgCatalogModule;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.settings.Setting;
import io.crate.metadata.settings.SettingsAppliers;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.monitor.MonitorModule;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.collect.CollectOperationModule;
import io.crate.operation.collect.files.FileCollectModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.reference.sys.check.SysChecksModule;
import io.crate.operation.reference.sys.check.node.SysNodeChecksModule;
import io.crate.operation.reference.sys.cluster.SysClusterExpressionModule;
import io.crate.operation.reference.sys.node.local.SysNodeExpressionModule;
import io.crate.operation.reference.sys.repositories.SysRepositoriesModule;
import io.crate.operation.reference.sys.repositories.SysRepositoriesService;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.tablefunctions.TableFunctionModule;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.rest.action.RestSQLAction;
import org.elasticsearch.action.bulk.BulkModule;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ArrayMapper;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;

public class SQLPlugin extends Plugin {

    private final Settings settings;

    public SQLPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Settings additionalSettings() {
        Settings.Builder settingsBuilder = Settings.settingsBuilder();

        // Set default analyzer
        settingsBuilder.put("index.analysis.analyzer.default.type", "keyword");

        // Never allow implicit creation of an index, even on partitioned tables we are creating
        // partitions explicitly
        settingsBuilder.put("action.auto_create_index", false);

        return settingsBuilder.build();
    }

    public String name() {
        return "sql";
    }

    public String description() {
        return "plugin that adds an /_sql endpoint to query crate with sql";
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        return ImmutableList.<Class<? extends LifecycleComponent>>of(
            DecommissioningService.class,
            BulkRetryCoordinatorPool.class,
            NodeDisconnectJobMonitorService.class,
            PostgresNetty.class,
            JobContextService.class,
            Schemas.class,
            SysRepositoriesService.class);
    }

    @Override
    public Collection<Module> nodeModules() {
        Collection<Module> modules = newArrayList();
        modules.add(new SQLModule());

        modules.add(new CircuitBreakerModule());
        modules.add(new TransportExecutorModule());
        modules.add(new JobModule());
        modules.add(new CollectOperationModule());
        modules.add(new FileCollectModule());
        modules.add(new MetaDataModule());
        modules.add(new MetaDataSysModule());
        modules.add(new MetaDataBlobModule());
        modules.add(new PgCatalogModule());
        modules.add(new MetaDataInformationModule());
        modules.add(new OperatorModule());
        modules.add(new PredicateModule());
        modules.add(new MonitorModule(settings));
        modules.add(new SysClusterExpressionModule());
        modules.add(new SysNodeExpressionModule());
        modules.add(new AggregationImplModule());
        modules.add(new ScalarFunctionModule());
        modules.add(new TableFunctionModule());
        modules.add(new BulkModule());
        modules.add(new SysChecksModule());
        modules.add(new SysNodeChecksModule());
        modules.add(new RepositorySettingsModule());
        modules.add(new SysRepositoriesModule());
        return modules;
    }


    @Override
    public Collection<Module> indexModules(Settings indexSettings) {
        Collection<Module> modules = newArrayList();
        if (!settings.getAsBoolean("node.client", false)) {
            modules.add(new CrateIndexModule());
        }
        return modules;
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(RestSQLAction.class);
    }

    public void onModule(ClusterModule clusterModule) {
        // add our dynamic cluster settings
        clusterModule.registerClusterDynamicSetting(Constants.CUSTOM_ANALYSIS_SETTINGS_PREFIX + "*", Validator.EMPTY);

        clusterModule.registerClusterDynamicSetting(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING, Validator.MEMORY_SIZE);
        clusterModule.registerClusterDynamicSetting(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING, Validator.NON_NEGATIVE_DOUBLE);

        clusterModule.registerClusterDynamicSetting("crate.internal.decommission.*", Validator.EMPTY);

        registerSettings(clusterModule, CrateSettings.CRATE_SETTINGS);

        clusterModule.registerAllocationDecider(DecommissionAllocationDecider.class);
    }

    private void registerSettings(ClusterModule clusterModule, Collection<? extends Setting> settings) {
        for (Setting setting : settings) {
            /**
             * validation is done in
             * {@link SettingsAppliers.AbstractSettingsApplier#apply(org.elasticsearch.common.settings.Settings.Builder, Object[], io.crate.sql.tree.Expression)}
             * here we use Validator.EMPTY, since ES ignores invalid settings silently
             **/

            clusterModule.registerClusterDynamicSetting(setting.settingName(), Validator.EMPTY);
            registerSettings(clusterModule, setting.children());
        }
    }

    public void onModule(IndicesModule indicesModule) {
        indicesModule.registerMapper(ArrayMapper.CONTENT_TYPE, new ArrayMapper.TypeParser());
    }
}

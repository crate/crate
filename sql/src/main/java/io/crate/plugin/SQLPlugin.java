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
import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLBulkAction;
import io.crate.action.sql.TransportSQLAction;
import io.crate.action.sql.TransportSQLBulkAction;
import io.crate.breaker.CircuitBreakerModule;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.executor.transport.TransportExecutorModule;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.blob.MetaDataBlobModule;
import io.crate.metadata.doc.MetaDataDocModule;
import io.crate.metadata.doc.array.ArrayMapperIndexModule;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.settings.Setting;
import io.crate.metadata.shard.MetaDataShardModule;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.jobs.JobContextService;
import io.crate.operation.collect.CollectOperationModule;
import io.crate.operation.collect.CollectShardModule;
import io.crate.operation.merge.MergeOperationModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.reference.sys.cluster.SysClusterExpressionModule;
import io.crate.operation.reference.sys.node.SysNodeExpressionModule;
import io.crate.operation.reference.sys.shard.SysShardExpressionModule;
import io.crate.operation.reference.sys.shard.blob.BlobShardExpressionModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.scalar.elasticsearch.script.NumericScalarSearchScript;
import io.crate.operation.scalar.elasticsearch.script.NumericScalarSortScript;
import io.crate.rest.action.RestSQLAction;
import io.crate.service.SQLService;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.bulk.BulkModule;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.settings.ClusterDynamicSettingsModule;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.script.ScriptModule;

import java.util.Collection;
import java.util.List;

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
        return ImmutableList.<Class<? extends LifecycleComponent>>of(
                SQLService.class,
                BulkRetryCoordinatorPool.class,
                JobContextService.class);
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        modules.add(SQLModule.class);

        modules.add(CircuitBreakerModule.class);
        modules.add(TransportExecutorModule.class);
        modules.add(CollectOperationModule.class);
        modules.add(MergeOperationModule.class);
        modules.add(MetaDataModule.class);
        modules.add(MetaDataSysModule.class);
        modules.add(MetaDataDocModule.class);
        modules.add(MetaDataBlobModule.class);
        modules.add(MetaDataInformationModule.class);
        modules.add(OperatorModule.class);
        modules.add(PredicateModule.class);
        modules.add(SysClusterExpressionModule.class);
        modules.add(SysNodeExpressionModule.class);
        modules.add(AggregationImplModule.class);
        modules.add(ScalarFunctionModule.class);
        modules.add(BulkModule.class);
        return modules;
    }


    @Override
    public Collection<Class<? extends Module>> indexModules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        if (!settings.getAsBoolean("node.client", false)) {
            modules.add(ArrayMapperIndexModule.class);
        }
        return modules;
    }

    @Override
    public Collection<Class<? extends Module>> shardModules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        if (!settings.getAsBoolean("node.client", false)) {
            modules.add(MetaDataShardModule.class);
            modules.add(SysShardExpressionModule.class);
            modules.add(BlobShardExpressionModule.class);
            modules.add(CollectShardModule.class);
        }
        return modules;
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(RestSQLAction.class);
    }

    public void onModule(ClusterDynamicSettingsModule clusterDynamicSettingsModule) {
        // add our dynamic cluster settings
        clusterDynamicSettingsModule.addDynamicSettings(Constants.CUSTOM_ANALYSIS_SETTINGS_PREFIX + "*");
        clusterDynamicSettingsModule.addDynamicSettings(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING);
        clusterDynamicSettingsModule.addDynamicSettings(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING);
        registerSettings(clusterDynamicSettingsModule, CrateSettings.CRATE_SETTINGS);
    }

    private void registerSettings(ClusterDynamicSettingsModule clusterDynamicSettingsModule, List<Setting> settings) {
        for (Setting setting : settings) {
            /**
             * validation is done in
             * {@link io.crate.analyze.SettingsAppliers.AbstractSettingsApplier#apply(org.elasticsearch.common.settings.ImmutableSettings.Builder, Object[], io.crate.sql.tree.Expression)}
             * here we use Validator.EMPTY, since ES ignores invalid settings silently
             */
            clusterDynamicSettingsModule.addDynamicSetting(setting.settingName(), Validator.EMPTY);
            registerSettings(clusterDynamicSettingsModule, setting.children());
        }
    }

    public void onModule(ScriptModule scriptModule) {
        NumericScalarSearchScript.register(scriptModule);
        NumericScalarSortScript.register(scriptModule);
    }


    public void onModule(ActionModule actionModule) {
        actionModule.registerAction(SQLAction.INSTANCE, TransportSQLAction.class);
        actionModule.registerAction(SQLBulkAction.INSTANCE, TransportSQLBulkAction.class);
    }
}

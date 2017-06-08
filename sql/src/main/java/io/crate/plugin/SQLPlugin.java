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
import io.crate.action.sql.SQLOperations;
import io.crate.analyze.repositories.RepositorySettingsModule;
import io.crate.breaker.CircuitBreakerModule;
import io.crate.cluster.gracefulstop.DecommissionAllocationDecider;
import io.crate.cluster.gracefulstop.DecommissioningService;
import io.crate.executor.transport.TransportExecutorModule;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobModule;
import io.crate.jobs.transport.NodeDisconnectJobMonitorService;
import io.crate.lucene.ArrayMapperService;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.MetaDataBlobModule;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.metadata.pg_catalog.PgCatalogModule;
import io.crate.metadata.settings.AnalyzerSettings;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.monitor.MonitorModule;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.auth.AuthenticationProvider;
import io.crate.operation.collect.CollectOperationModule;
import io.crate.operation.collect.files.FileCollectModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.reference.sys.check.SysChecksModule;
import io.crate.operation.reference.sys.check.node.SysNodeChecksModule;
import io.crate.operation.reference.sys.cluster.SysClusterExpressionModule;
import io.crate.operation.reference.sys.node.local.SysNodeExpressionModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.tablefunctions.TableFunctionModule;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.rest.action.RestSQLAction;
import io.crate.settings.CrateSetting;
import org.elasticsearch.action.bulk.BulkModule;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ArrayMapper;
import org.elasticsearch.index.mapper.ArrayTypeParser;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

public class SQLPlugin extends Plugin implements ActionPlugin, MapperPlugin, ClusterPlugin {

    private final Settings settings;

    @SuppressWarnings("WeakerAccess") // must be public for pluginLoader
    public SQLPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Settings additionalSettings() {
        Settings.Builder settingsBuilder = Settings.builder();

        // Never allow implicit creation of an index, even on partitioned tables we are creating
        // partitions explicitly
        settingsBuilder.put("action.auto_create_index", false);

        return settingsBuilder.build();
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.add(AnalyzerSettings.CUSTOM_ANALYSIS_SETTING_GROUP);
        settings.add(SQLOperations.NODE_READ_ONLY_SETTING);

        // Postgres settings are node settings
        settings.add(PostgresNetty.PSQL_ENABLED_SETTING.setting());
        settings.add(PostgresNetty.PSQL_PORT_SETTING.setting());

        // Authentication settings are node settings
        settings.add(AuthenticationProvider.AUTH_HOST_BASED_ENABLED_SETTING.setting());
        settings.add(AuthenticationProvider.AUTH_HOST_BASED_CONFIG_SETTING.setting());
        settings.add(AuthenticationProvider.AUTH_TRUST_HTTP_DEFAULT_HEADER.setting());

        // also add CrateSettings
        for (CrateSetting crateSetting : CrateSettings.CRATE_CLUSTER_SETTINGS) {
            settings.add(crateSetting.setting());
        }

        return settings;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return ImmutableList.of(
            DecommissioningService.class,
            NodeDisconnectJobMonitorService.class,
            PostgresNetty.class,
            JobContextService.class,
            Schemas.class,
            ArrayMapperService.class);
    }

    @Override
    public Collection<Module> createGuiceModules() {
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
        return modules;
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        return Collections.singletonList(RestSQLAction.class);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(ArrayMapper.CONTENT_TYPE, new ArrayTypeParser());
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return ImmutableList.of(new DecommissionAllocationDecider(settings, clusterSettings));
    }
}

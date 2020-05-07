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
import io.crate.auth.AuthSettings;
import io.crate.breaker.CircuitBreakerModule;
import io.crate.cluster.gracefulstop.DecommissionAllocationDecider;
import io.crate.cluster.gracefulstop.DecommissioningService;
import io.crate.execution.TransportExecutorModule;
import io.crate.execution.engine.aggregation.impl.AggregationImplModule;
import io.crate.execution.engine.collect.CollectOperationModule;
import io.crate.execution.engine.collect.files.FileCollectModule;
import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.execution.engine.window.WindowFunctionModule;
import io.crate.execution.jobs.JobModule;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.transport.NodeDisconnectJobMonitorService;
import io.crate.expression.operator.OperatorModule;
import io.crate.expression.predicate.PredicateModule;
import io.crate.expression.reference.sys.check.SysChecksModule;
import io.crate.expression.reference.sys.check.node.SysNodeChecksModule;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.tablefunctions.TableFunctionModule;
import io.crate.expression.udf.UserDefinedFunctionsMetaData;
import io.crate.license.CeLicenseModule;
import io.crate.license.LicenseExtension;
import io.crate.lucene.ArrayMapperService;
import io.crate.metadata.DanglingArtifactsService;
import io.crate.metadata.DefaultTemplateService;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.MetaDataBlobModule;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.metadata.pgcatalog.PgCatalogModule;
import io.crate.metadata.settings.AnalyzerSettings;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.upgrade.IndexTemplateUpgrader;
import io.crate.metadata.upgrade.MetaDataIndexUpgrader;
import io.crate.metadata.view.ViewsMetaData;
import io.crate.monitor.MonitorModule;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.protocols.ssl.SslConfigSettings;
import io.crate.protocols.ssl.SslContextProviderFallbackModule;
import io.crate.protocols.ssl.SslExtension;
import io.crate.settings.CrateSetting;
import io.crate.user.UserExtension;
import io.crate.user.UserFallbackModule;
import org.elasticsearch.action.bulk.BulkModule;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.ArrayMapper;
import org.elasticsearch.index.mapper.ArrayTypeParser;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static com.google.common.collect.Lists.newArrayList;

public class SQLPlugin extends Plugin implements ActionPlugin, MapperPlugin, ClusterPlugin {

    private final Settings settings;
    @Nullable
    private final UserExtension userExtension;
    @Nullable
    private final LicenseExtension licenseExtension;
    @Nullable
    private final SslExtension sslExtension;

    public SQLPlugin(Settings settings) {
        this.settings = settings;
        userExtension = EnterpriseLoader.loadSingle(UserExtension.class);
        licenseExtension = EnterpriseLoader.loadSingle(LicenseExtension.class);
        sslExtension = EnterpriseLoader.loadSingle(SslExtension.class);
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
        settings.add(AuthSettings.AUTH_HOST_BASED_ENABLED_SETTING.setting());
        settings.add(AuthSettings.AUTH_HOST_BASED_CONFIG_SETTING.setting());
        settings.add(AuthSettings.AUTH_TRUST_HTTP_DEFAULT_HEADER.setting());

        // Settings for SSL (available only in the Enterprise version)
        settings.add(SslConfigSettings.SSL_HTTP_ENABLED.setting());
        settings.add(SslConfigSettings.SSL_PSQL_ENABLED.setting());
        settings.add(SslConfigSettings.SSL_TRUSTSTORE_FILEPATH.setting());
        settings.add(SslConfigSettings.SSL_TRUSTSTORE_PASSWORD.setting());
        settings.add(SslConfigSettings.SSL_KEYSTORE_FILEPATH.setting());
        settings.add(SslConfigSettings.SSL_KEYSTORE_PASSWORD.setting());
        settings.add(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD.setting());
        settings.add(SslConfigSettings.SSL_RESOURCE_POLL_INTERVAL.setting());

        // also add CrateSettings
        for (CrateSetting crateSetting : CrateSettings.CRATE_CLUSTER_SETTINGS) {
            settings.add(crateSetting.setting());
        }

        return settings;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        ImmutableList.Builder<Class<? extends LifecycleComponent>> builder =
            ImmutableList.<Class<? extends LifecycleComponent>>builder()
            .add(DecommissioningService.class)
            .add(NodeDisconnectJobMonitorService.class)
            .add(JobsLogService.class)
            .add(PostgresNetty.class)
            .add(TasksService.class)
            .add(Schemas.class)
            .add(DefaultTemplateService.class)
            .add(ArrayMapperService.class)
            .add(DanglingArtifactsService.class);
        if (licenseExtension != null) {
            builder.addAll(licenseExtension.getGuiceServiceClasses());
        }
        if (sslExtension != null) {
            builder.addAll(sslExtension.getGuiceServiceClasses());
        }
        return builder.build();
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
        modules.add(new MonitorModule());
        modules.add(new AggregationImplModule());
        modules.add(new ScalarFunctionModule());
        modules.add(new TableFunctionModule());
        modules.add(new WindowFunctionModule());
        modules.add(new BulkModule());
        modules.add(new SysChecksModule());
        modules.add(new SysNodeChecksModule());
        if (userExtension != null) {
            modules.addAll(userExtension.getModules(settings));
        } else {
            modules.add(new UserFallbackModule());
        }
        if (licenseExtension != null) {
            modules.addAll(licenseExtension.getModules(settings));
        } else {
            modules.add(new CeLicenseModule());
        }
        if (sslExtension != null) {
            modules.addAll(sslExtension.getModules());
        } else {
            modules.add(new SslContextProviderFallbackModule());
        }
        return modules;
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(ArrayMapper.CONTENT_TYPE, new ArrayTypeParser());
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return ImmutableList.of(new DecommissionAllocationDecider(settings, clusterSettings));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedWriteableRegistry.Entry(
            MetaData.Custom.class,
            UserDefinedFunctionsMetaData.TYPE,
            UserDefinedFunctionsMetaData::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            MetaData.Custom.class,
            ViewsMetaData.TYPE,
            ViewsMetaData::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            UserDefinedFunctionsMetaData.TYPE,
            in -> UserDefinedFunctionsMetaData.readDiffFrom(MetaData.Custom.class, UserDefinedFunctionsMetaData.TYPE, in)
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            ViewsMetaData.TYPE,
            in -> ViewsMetaData.readDiffFrom(MetaData.Custom.class, ViewsMetaData.TYPE, in)
        ));
        if (userExtension != null) {
            entries.addAll(userExtension.getNamedWriteables());
        }
        if (licenseExtension != null) {
            entries.addAll(licenseExtension.getNamedWriteables());
        }
        return entries;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedXContentRegistry.Entry(
            MetaData.Custom.class,
            new ParseField(UserDefinedFunctionsMetaData.TYPE),
            UserDefinedFunctionsMetaData::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            MetaData.Custom.class,
            new ParseField(ViewsMetaData.TYPE),
            ViewsMetaData::fromXContent
        ));

        if (userExtension != null) {
            entries.addAll(userExtension.getNamedXContent());
        }
        if (licenseExtension != null) {
            entries.addAll(licenseExtension.getNamedXContent());
        }
        return entries;
    }

    @Override
    public UnaryOperator<IndexMetaData> getIndexMetaDataUpgrader() {
        return new MetaDataIndexUpgrader();
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
        return new IndexTemplateUpgrader();
    }
}

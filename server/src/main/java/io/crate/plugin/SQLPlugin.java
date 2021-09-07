/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import io.crate.action.sql.SQLOperations;
import io.crate.auth.AuthSettings;
import io.crate.auth.AuthenticationModule;
import io.crate.cluster.gracefulstop.DecommissionAllocationDecider;
import io.crate.cluster.gracefulstop.DecommissioningService;
import io.crate.execution.TransportExecutorModule;
import io.crate.execution.engine.collect.CollectOperationModule;
import io.crate.execution.engine.collect.files.FileCollectModule;
import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.execution.jobs.JobModule;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.transport.NodeDisconnectJobMonitorService;
import io.crate.expression.operator.OperatorModule;
import io.crate.expression.predicate.PredicateModule;
import io.crate.expression.reference.sys.check.SysChecksModule;
import io.crate.expression.reference.sys.check.node.SysNodeChecksModule;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.license.License;
import io.crate.lucene.ArrayMapperService;
import io.crate.metadata.CustomMetadataUpgraderLoader;
import io.crate.metadata.DanglingArtifactsService;
import io.crate.metadata.DefaultTemplateService;
import io.crate.metadata.MetadataModule;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.MetadataBlobModule;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.action.GetFileChunkAction;
import io.crate.replication.logical.action.GetStoreMetadataAction;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.action.ReleasePublisherResourcesAction;
import io.crate.replication.logical.action.ReplayChangesAction;
import io.crate.replication.logical.action.ShardChangesAction;
import io.crate.replication.logical.engine.SubscriberEngine;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.metadata.information.MetadataInformationModule;
import io.crate.metadata.pgcatalog.PgCatalogModule;
import io.crate.metadata.settings.AnalyzerSettings;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.sys.MetadataSysModule;
import io.crate.metadata.upgrade.IndexTemplateUpgrader;
import io.crate.metadata.upgrade.MetadataIndexUpgrader;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.module.CrateCommonModule;
import io.crate.monitor.MonitorModule;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.protocols.ssl.SslContextProviderService;
import io.crate.protocols.ssl.SslSettings;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.user.UserManagementModule;
import io.crate.user.metadata.UsersMetadata;
import io.crate.user.metadata.UsersPrivilegesMetadata;
import org.elasticsearch.action.bulk.BulkModule;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.Custom;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.ArrayMapper;
import org.elasticsearch.index.mapper.ArrayTypeParser;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;


public class SQLPlugin extends Plugin implements ActionPlugin, MapperPlugin, ClusterPlugin, EnginePlugin {

    private final Settings settings;
    @Nullable
    private final IndexEventListenerProxy indexEventListenerProxy;

    public SQLPlugin(Settings settings) {
        this.settings = settings;
        this.indexEventListenerProxy = new IndexEventListenerProxy();
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.add(AnalyzerSettings.CUSTOM_ANALYSIS_SETTING_GROUP);
        settings.add(SQLOperations.NODE_READ_ONLY_SETTING);

        // Postgres settings are node settings
        settings.add(PostgresNetty.PSQL_ENABLED_SETTING);
        settings.add(PostgresNetty.PSQL_PORT_SETTING);

        // Authentication settings are node settings
        settings.add(AuthSettings.AUTH_HOST_BASED_ENABLED_SETTING);
        settings.add(AuthSettings.AUTH_HOST_BASED_CONFIG_SETTING);
        settings.add(AuthSettings.AUTH_TRUST_HTTP_DEFAULT_HEADER);

        // Settings for SSL
        settings.add(SslSettings.SSL_TRANSPORT_MODE);
        settings.add(SslSettings.SSL_HTTP_ENABLED);
        settings.add(SslSettings.SSL_PSQL_ENABLED);
        settings.add(SslSettings.SSL_TRUSTSTORE_FILEPATH);
        settings.add(SslSettings.SSL_TRUSTSTORE_PASSWORD);
        settings.add(SslSettings.SSL_KEYSTORE_FILEPATH);
        settings.add(SslSettings.SSL_KEYSTORE_PASSWORD);
        settings.add(SslSettings.SSL_KEYSTORE_KEY_PASSWORD);
        settings.add(SslSettings.SSL_RESOURCE_POLL_INTERVAL);

        // Logical replication
        settings.add(LogicalReplicationService.REPLICATION_SUBSCRIBED_INDEX);

        settings.addAll(CrateSettings.CRATE_CLUSTER_SETTINGS);

        return settings;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return List.<Class<? extends LifecycleComponent>>of(
            DecommissioningService.class,
            NodeDisconnectJobMonitorService.class,
            JobsLogService.class,
            PostgresNetty.class,
            TasksService.class,
            Schemas.class,
            DefaultTemplateService.class,
            ArrayMapperService.class,
            DanglingArtifactsService.class,
            SslContextProviderService.class
        );
    }

    @Override
    public Collection<Module> createGuiceModules() {
        ArrayList<Module> modules = new ArrayList<>();
        modules.add(new SQLModule());

        modules.add(new CrateCommonModule(indexEventListenerProxy));
        modules.add(new TransportExecutorModule());
        modules.add(new JobModule());
        modules.add(new CollectOperationModule());
        modules.add(new FileCollectModule());
        modules.add(new MetadataModule());
        modules.add(new MetadataSysModule());
        modules.add(new MetadataBlobModule());
        modules.add(new PgCatalogModule());
        modules.add(new MetadataInformationModule());
        modules.add(new OperatorModule());
        modules.add(new PredicateModule());
        modules.add(new MonitorModule());
        modules.add(new BulkModule());
        modules.add(new SysChecksModule());
        modules.add(new SysNodeChecksModule());
        modules.add(new UserManagementModule());
        modules.add(new AuthenticationModule());
        return modules;
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(ArrayMapper.CONTENT_TYPE, new ArrayTypeParser());
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return List.of(new DecommissionAllocationDecider(settings, clusterSettings));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            UserDefinedFunctionsMetadata.TYPE,
            UserDefinedFunctionsMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            ViewsMetadata.TYPE,
            ViewsMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            UserDefinedFunctionsMetadata.TYPE,
            in -> UserDefinedFunctionsMetadata.readDiffFrom(Metadata.Custom.class, UserDefinedFunctionsMetadata.TYPE, in)
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            ViewsMetadata.TYPE,
            in -> ViewsMetadata.readDiffFrom(Metadata.Custom.class, ViewsMetadata.TYPE, in)
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            UsersMetadata.TYPE,
            UsersMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            UsersMetadata.TYPE,
            in -> UsersMetadata.readDiffFrom(Metadata.Custom.class, UsersMetadata.TYPE, in)
        ));

        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            UsersPrivilegesMetadata.TYPE,
            UsersPrivilegesMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            UsersPrivilegesMetadata.TYPE,
            in -> UsersPrivilegesMetadata.readDiffFrom(Metadata.Custom.class, UsersPrivilegesMetadata.TYPE, in)
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            PublicationsMetadata.TYPE,
            PublicationsMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            PublicationsMetadata.TYPE,
            in -> PublicationsMetadata.readDiffFrom(Metadata.Custom.class, PublicationsMetadata.TYPE, in)
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            Metadata.Custom.class,
            SubscriptionsMetadata.TYPE,
            SubscriptionsMetadata::new
        ));
        entries.add(new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            SubscriptionsMetadata.TYPE,
            in -> SubscriptionsMetadata.readDiffFrom(Metadata.Custom.class, SubscriptionsMetadata.TYPE, in)
        ));

        //Only kept for bwc reasons to make sure we can read from a CrateDB < 4.5 node
        entries.addAll(License.getNamedWriteables());
        return entries;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(UserDefinedFunctionsMetadata.TYPE),
            UserDefinedFunctionsMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(ViewsMetadata.TYPE),
            ViewsMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(UsersMetadata.TYPE),
            UsersMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(UsersPrivilegesMetadata.TYPE),
            UsersPrivilegesMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(PublicationsMetadata.TYPE),
            PublicationsMetadata::fromXContent
        ));
        entries.add(new NamedXContentRegistry.Entry(
            Metadata.Custom.class,
            new ParseField(SubscriptionsMetadata.TYPE),
            SubscriptionsMetadata::fromXContent
        ));
        //Only kept for bwc reasons to make sure we can read from a CrateDB < 4.5 node
        entries.addAll(License.getNamedXContent());
        return entries;
    }

    @Override
    public UnaryOperator<IndexMetadata> getIndexMetadataUpgrader() {
        return new MetadataIndexUpgrader();
    }

    @Override
    public UnaryOperator<Map<String, Custom>> getCustomMetadataUpgrader() {
        return new CustomMetadataUpgraderLoader(settings);
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
        return new IndexTemplateUpgrader();
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        indexModule.addIndexEventListener(indexEventListenerProxy);
    }

    @Override
    public List<ActionHandler<? extends TransportRequest, ? extends TransportResponse>> getActions() {
        return List.of(
            new ActionHandler<>(RetentionLeaseActions.Add.INSTANCE, RetentionLeaseActions.Add.TransportAction.class),
            new ActionHandler<>(RetentionLeaseActions.Remove.INSTANCE, RetentionLeaseActions.Remove.TransportAction.class),
            new ActionHandler<>(RetentionLeaseActions.Renew.INSTANCE, RetentionLeaseActions.Renew.TransportAction.class),
            new ActionHandler<>(PublicationsStateAction.INSTANCE, PublicationsStateAction.TransportAction.class),
            new ActionHandler<>(GetFileChunkAction.INSTANCE, GetFileChunkAction.TransportAction.class),
            new ActionHandler<>(GetStoreMetadataAction.INSTANCE, GetStoreMetadataAction.TransportAction.class),
            new ActionHandler<>(ReleasePublisherResourcesAction.INSTANCE, ReleasePublisherResourcesAction.TransportAction.class),
            new ActionHandler<>(ShardChangesAction.INSTANCE, ShardChangesAction.TransportAction.class),
            new ActionHandler<>(ReplayChangesAction.INSTANCE, ReplayChangesAction.TransportAction.class)
        );
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getSettings().get(LogicalReplicationService.REPLICATION_SUBSCRIBED_INDEX.getKey()) != null) {
            return Optional.of(SubscriberEngine::new);
        }
        return Optional.empty();
    }


}

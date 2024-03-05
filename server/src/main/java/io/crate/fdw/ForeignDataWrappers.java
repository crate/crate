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

package io.crate.fdw;

import static java.util.Objects.requireNonNull;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.RelationUnknown;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.ForeignCollectPhase;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.sources.CollectSource;
import io.crate.expression.InputFactory;
import io.crate.fdw.ServersMetadata.Server;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.role.Roles;

@Singleton
public class ForeignDataWrappers implements CollectSource {

    public static Setting<Boolean> ALLOW_LOCAL = Setting.boolSetting(
        "fdw.allow_local",
        false,
        Property.NodeScope,
        Property.Exposed
    );

    private final ClusterService clusterService;
    private final InputFactory inputFactory;
    private final Map<String, ForeignDataWrapper> wrappers;
    private final Roles roles;

    @Inject
    public ForeignDataWrappers(Settings settings,
                               ClusterService clusterService,
                               NodeContext nodeContext) {
        this.clusterService = clusterService;
        this.inputFactory = new InputFactory(nodeContext);
        this.wrappers = Map.of(
            "jdbc", new JdbcForeignDataWrapper(settings, inputFactory)
        );
        this.roles = nodeContext.roles();
    }

    public boolean contains(String fdw) {
        return wrappers.containsKey(fdw);
    }

    public ForeignDataWrapper get(String fdw) {
        var foreignDataWrapper = wrappers.get(fdw);
        if (foreignDataWrapper == null) {
            throw new IllegalArgumentException(
                "foreign-data wrapper " + fdw + " does not exist");
        }
        return foreignDataWrapper;
    }

    @Override
    public CompletableFuture<BatchIterator<Row>> getIterator(TransactionContext txnCtx,
                                                             CollectPhase collectPhase,
                                                             CollectTask collectTask,
                                                             boolean supportMoveToStart) {
        if (!(collectPhase instanceof ForeignCollectPhase phase)) {
            throw new IllegalArgumentException(
                "ForeignDataWrappers requires ForeignCollectPhase, not: " + collectPhase);
        }
        Metadata metadata = clusterService.state().metadata();
        ForeignTablesMetadata foreignTables = metadata.custom(ForeignTablesMetadata.TYPE);
        if (foreignTables == null) {
            throw new RelationUnknown(phase.relationName());
        }
        ForeignTable foreignTable = foreignTables.get(phase.relationName());
        if (foreignTable == null) {
            throw new RelationUnknown(phase.relationName());
        }
        ServersMetadata servers = metadata.custom(ServersMetadata.TYPE);
        if (servers == null) {
            throw new ResourceNotFoundException(
                String.format(Locale.ENGLISH, "Server `%s` not found", foreignTable.server()));
        }
        Server server = servers.get(foreignTable.server());
        ForeignDataWrapper fdw = wrappers.get(server.fdw());
        if (fdw == null) {
            throw new ResourceNotFoundException(String.format(
                Locale.ENGLISH,
                "Foreign data wrapper '%s' used by server '%s' no longer exists",
                server.fdw(),
                foreignTable.server()
            ));
        }
        return fdw.getIterator(
            requireNonNull(roles.findUser(txnCtx.sessionSettings().userName()), "current user must exit"),
            server,
            foreignTable,
            txnCtx,
            collectPhase.toCollect(),
            phase.query()
        );
    }
}

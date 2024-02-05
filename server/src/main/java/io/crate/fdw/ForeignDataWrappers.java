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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.RelationUnknown;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.ForeignCollectPhase;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.sources.CollectSource;
import io.crate.execution.engine.pipeline.InputRowProjector;
import io.crate.expression.InputFactory;
import io.crate.expression.InputFactory.Context;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.fdw.ServersMetadata.Server;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.settings.SessionSettings;
import io.crate.types.DataTypes;

@Singleton
public class ForeignDataWrappers implements CollectSource {

    private final ClusterService clusterService;
    private final InputFactory inputFactory;

    private final Map<String, ForeignDataWrapper> wrappers = Map.of(
        "jdbc", new ForeignDataWrapper() {

            @Override
            public CompletableFuture<BatchIterator<Row>> getIterator(Server server,
                                                                     TransactionContext txnCtx,
                                                                     RelationName relationName,
                                                                     List<Symbol> collect) {
                SessionSettings sessionSettings = txnCtx.sessionSettings();
                Map<String, Object> userOptions = server.users().get(sessionSettings.userName());
                if (userOptions == null) {
                    userOptions = Map.of();
                }
                String user = DataTypes.STRING.implicitCast(userOptions.get("user"));
                String password = DataTypes.STRING.implicitCast(userOptions.get("password"));
                var properties = new Properties();
                properties.setProperty("user", user == null ? sessionSettings.userName() : user);
                if (password != null) {
                    properties.setProperty("password", password);
                }

                // It's unknown if/what kind of scalars are supported by the remote.
                // Evaluate them locally and only fetch columns
                List<Reference> refs = new ArrayList<>(collect.size());
                for (var symbol : collect) {
                    RefVisitor.visitRefs(symbol, ref -> refs.add(ref));
                }

                Map<String, Object> options = server.options();
                Object urlObject = options.getOrDefault("url", "jdbc:postgresql://localhost:5432/");
                String url = DataTypes.STRING.implicitCast(urlObject);
                BatchIterator<Row> it = new JdbcBatchIterator(url, properties, refs, relationName);
                if (!refs.containsAll(collect)) {
                    var sourceRefs = new InputColumns.SourceSymbols(refs);
                    List<Symbol> inputColumns = InputColumns.create(collect, sourceRefs);
                    Context<CollectExpression<Row, ?>> inputCtx = inputFactory.ctxForInputColumns(txnCtx, inputColumns);
                    InputRowProjector inputRowProjector = new InputRowProjector(inputCtx.topLevelInputs(), inputCtx.expressions());
                    it = inputRowProjector.apply(it);
                }
                return CompletableFuture.completedFuture(it);
            }
        }
    );

    @Inject
    public ForeignDataWrappers(ClusterService clusterService, NodeContext nodeContext) {
        this.clusterService = clusterService;
        this.inputFactory = new InputFactory(nodeContext);
    }

    public boolean contains(String fdw) {
        return wrappers.containsKey(fdw);
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

        Map<String, Object> options = foreignTable.options();
        RelationName name = foreignTable.name();
        String remoteSchema = DataTypes.STRING.implicitCast(options.getOrDefault("schema_name", name.schema()));
        String remoteTable = DataTypes.STRING.implicitCast(options.getOrDefault("table_name", name.name()));
        RelationName remoteName = new RelationName(remoteSchema, remoteTable);
        return fdw.getIterator(
            server,
            txnCtx,
            remoteName,
            collectPhase.toCollect()
        );
    }
}

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

package io.crate.metadata;

import java.util.Map;
import java.util.function.Function;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.env.Environment;

import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfoFactory;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.view.ViewInfoFactory;
import io.crate.role.Roles;
import io.crate.statistics.TableStats;

public class NodeContext {

    private final Functions functions;
    private final long serverStartTimeInMs;
    private final Roles roles;
    private final Schemas schemas;

    public static NodeContext of(Environment environment,
                                 ClusterService clusterService,
                                 Functions functions,
                                 Roles roles,
                                 TableStats tableStats) {
        return new NodeContext(functions, roles, nodeCtx -> {
            var tableInfoFactory = new DocTableInfoFactory(nodeCtx);
            BlobSchemaInfo blobSchemaInfo = new BlobSchemaInfo(
                clusterService,
                new BlobTableInfoFactory(clusterService.getSettings(), environment));
            Map<String, SchemaInfo> systemSchemas = Map.of(
                SysSchemaInfo.NAME, new SysSchemaInfo(clusterService, nodeCtx.roles()),
                InformationSchemaInfo.NAME, new InformationSchemaInfo(),
                PgCatalogSchemaInfo.NAME, new PgCatalogSchemaInfo(tableStats, nodeCtx.roles()),
                BlobSchemaInfo.NAME, blobSchemaInfo
            );
            var relationAnalyzer = new RelationAnalyzer(nodeCtx);
            var viewInfoFactory = new ViewInfoFactory(relationAnalyzer);
            Schemas schemas = new Schemas(
                systemSchemas,
                clusterService,
                new DocSchemaInfoFactory(tableInfoFactory, viewInfoFactory),
                nodeCtx.roles()
            );
            schemas.start();
            return schemas;
        });
    }

    public NodeContext(Functions functions, Roles roles, Function<NodeContext, Schemas> createSchemas) {
        this.functions = functions;
        this.serverStartTimeInMs = SystemClock.currentInstant().toEpochMilli();
        this.roles = roles;
        this.schemas = createSchemas.apply(this);
    }

    public Functions functions() {
        return functions;
    }

    public long serverStartTimeInMs() {
        return serverStartTimeInMs;
    }

    public Roles roles() {
        return roles;
    }

    public Schemas schemas() {
        return schemas;
    }
}

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

package io.crate.analyze;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;

import io.crate.action.sql.SessionContext;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.DropBlobTable;
import io.crate.sql.tree.DropTable;
import io.crate.sql.tree.QualifiedName;

class DropTableAnalyzer {

    private static final Logger LOGGER = LogManager.getLogger(DropTableAnalyzer.class);

    private final Schemas schemas;
    private final ClusterService clusterService;

    DropTableAnalyzer(ClusterService clusterService, Schemas schemas) {
        this.clusterService = clusterService;
        this.schemas = schemas;
    }

    public AnalyzedDropTable<DocTableInfo> analyze(DropTable<?> node, SessionContext sessionContext) {
        return analyze(node.table().getName(), node.dropIfExists(), sessionContext);
    }

    public AnalyzedDropTable<BlobTableInfo> analyze(DropBlobTable<?> node, SessionContext sessionContext) {
        List<String> parts = node.table().getName().getParts();
        if (parts.size() != 1 && !parts.get(0).equals(BlobSchemaInfo.NAME)) {
            throw new IllegalArgumentException("No blob tables in schema `" + parts.get(0) + "`");
        } else {
            QualifiedName name = new QualifiedName(
                List.of(BlobSchemaInfo.NAME, node.table().getName().getSuffix()));
            return analyze(name, node.ignoreNonExistentTable(), sessionContext);
        }
    }

    private <T extends TableInfo> AnalyzedDropTable<T> analyze(QualifiedName name,
                                                               boolean dropIfExists,
                                                               SessionContext sessionContext) {
        T tableInfo;
        RelationName tableName;
        boolean maybeCorrupt = false;
        try {
            //noinspection unchecked
            tableInfo = (T) schemas.resolveTableInfo(name, Operation.DROP, sessionContext.sessionUser(), sessionContext.searchPath());
            tableName = tableInfo.ident();
        } catch (SchemaUnknownException | RelationUnknown e) {
            tableName = RelationName.of(name, sessionContext.searchPath().currentSchema());
            var metadata = clusterService.state().metadata();
            var indexNameOrAlias = tableName.indexNameOrAlias();

            if (metadata.hasIndex(indexNameOrAlias) || metadata.templates().containsKey(indexNameOrAlias)) {
                tableInfo = null;
                maybeCorrupt = true;
            } else if (dropIfExists) {
                tableInfo = null;
            } else {
                throw e;
            }
        } catch (OperationOnInaccessibleRelationException e) {
            throw e;
        } catch (Throwable t) {
            if (!sessionContext.sessionUser().isSuperUser()) {
                throw t;
            }
            tableInfo = null;
            maybeCorrupt = true;
            tableName = RelationName.of(name, sessionContext.searchPath().currentSchema());
            LOGGER.info(
                "Unexpected error resolving table during DROP TABLE operation on {}. " +
                "Proceeding with operation as table schema may be corrupt (error={})",
                tableName,
                t
            );
        }
        return new AnalyzedDropTable<>(tableInfo, dropIfExists, tableName, maybeCorrupt);
    }
}

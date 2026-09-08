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

import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableInfo;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.Operation;
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

    public AnalyzedDropTable analyze(DropTable node, CoordinatorSessionSettings sessionSettings) {
        List<AnalyzedDropTable.DropTableTarget> targets = new ArrayList<>();
        for (QualifiedName name : node.tables()) {
            targets.add(resolve(name, node.dropIfExists(), sessionSettings));
        }
        return new AnalyzedDropTable(node.dropIfExists(), targets);
    }


    public AnalyzedDropTable analyze(DropBlobTable<?> node, CoordinatorSessionSettings sessionSettings) {
        List<String> parts = node.table().getName().getParts();
        if (parts.size() != 1 && !parts.get(0).equals(BlobSchemaInfo.NAME)) {
            throw new IllegalArgumentException("No blob tables in schema `" + parts.get(0) + "`");
        } else {
            QualifiedName name = new QualifiedName(
                List.of(BlobSchemaInfo.NAME, node.table().getName().getSuffix()));
            var target = resolve(name, node.ignoreNonExistentTable(), sessionSettings);
            return new AnalyzedDropTable(node.ignoreNonExistentTable(), List.of(target));
        }
    }

    private AnalyzedDropTable.DropTableTarget resolve(QualifiedName name,
                                                      boolean dropIfExists,
                                                      CoordinatorSessionSettings sessionSettings) {
        RelationName tableName;
        int tableOid = OID_UNASSIGNED;
        try {
            TableInfo tableInfo = schemas.findRelation(
                name,
                Operation.DROP,
                sessionSettings.sessionUser(),
                sessionSettings.searchPath()
            );
            tableName = tableInfo.ident();
            tableOid = tableInfo.oid();
        } catch (SchemaUnknownException | RelationUnknown e) {
            tableName = RelationName.of(name, sessionSettings.searchPath().currentSchema());
            var metadata = clusterService.state().metadata();
            if (!(metadata.contains(tableName) || dropIfExists)) {
                throw e;
            }
        } catch (OperationOnInaccessibleRelationException e) {
            throw e;
        } catch (Throwable t) {
            if (!sessionSettings.sessionUser().isSuperUser()) {
                throw t;
            }
            tableName = RelationName.of(name, sessionSettings.searchPath().currentSchema());
            LOGGER.info(
                "Unexpected error resolving table during DROP TABLE operation on {}. " +
                "Proceeding with operation as table schema may be corrupt (error={})",
                tableName,
                t
            );
        }
        return new AnalyzedDropTable.DropTableTarget(tableName, tableOid);
    }
}

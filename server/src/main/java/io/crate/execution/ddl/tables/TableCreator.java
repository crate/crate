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

package io.crate.execution.ddl.tables;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.analyze.BoundCreateTable;
import io.crate.analyze.TableParameters;
import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.exceptions.SQLExceptions;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.SysColumns;
import io.crate.sql.tree.ColumnPolicy;

public class TableCreator {

    protected static final Logger LOGGER = LogManager.getLogger(TableCreator.class);
    private final NodeClient client;

    public TableCreator(NodeClient client) {
        this.client = client;
    }

    public CompletableFuture<Long> create(BoundCreateTable createTable, Version minNodeVersion) {
        var relationName = createTable.tableName();
        CreateTableRequest createTableRequest;

        Map<ColumnIdent, Reference> references = createTable.columns();
        IntArrayList pKeysIndices = createTable.primaryKeysIndices();
        ColumnPolicy tableColumnPolicy = TableParameters.COLUMN_POLICY.get(createTable.settings());
        Settings.Builder settingsBuilder = Settings.builder()
            .put(createTable.settings());
        settingsBuilder.remove(TableParameters.COLUMN_POLICY.getKey());
        ColumnIdent routingColumn = createTable.routingColumn().equals(SysColumns.ID.COLUMN) ? null : createTable.routingColumn();
        if (minNodeVersion.onOrAfter(Version.V_5_4_0)) {
            createTableRequest = new CreateTableRequest(
                relationName,
                createTable.pkConstraintName(),
                new ArrayList<>(references.values()),
                pKeysIndices,
                createTable.getCheckConstraints(),
                settingsBuilder.build(),
                routingColumn,
                tableColumnPolicy,
                createTable.partitionedBy()
            );
        } else {
            throw new UnsupportedOperationException("All nodes in the cluster must at least have version 5.4.0");
        }
        return client.execute(TransportCreateTableAction.ACTION, createTableRequest).thenApply(resp -> {
            if (!resp.isAllShardsAcked() && LOGGER.isWarnEnabled()) {
                LOGGER.warn("CREATE TABLE `{}` was not acknowledged. This could lead to inconsistent state.", relationName.fqn());
            }
            return 1L;
        }).exceptionally(error -> {
            Throwable t = SQLExceptions.unwrap(error);
            String message = t.getMessage();
            Throwable cause = t.getCause();
            if ("mapping [default]".equals(message) && cause != null) {
                // this is a generic mapping parse exception,
                // the cause has usually a better more detailed error message
                throw Exceptions.toRuntimeException(cause);
            } else {
                throw Exceptions.toRuntimeException(t);
            }
        });
    }

    public static boolean isTableExistsError(Throwable t, @Nullable String templateName) {
        t = SQLExceptions.unwrap(t);
        return t instanceof ResourceAlreadyExistsException
            || t instanceof RelationAlreadyExists
            || (templateName != null && isTemplateAlreadyExistsException(t));
    }

    private static boolean isTemplateAlreadyExistsException(Throwable t) {
        return t instanceof IllegalArgumentException
            && t.getMessage() != null && t.getMessage().endsWith("already exists");
    }
}

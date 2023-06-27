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

import static io.crate.execution.ddl.tables.MappingUtil.createMapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.analyze.BoundCreateTable;
import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.table.ColumnPolicies;
import io.crate.sql.tree.ColumnPolicy;

@Singleton
public class TableCreator {

    protected static final Logger LOGGER = LogManager.getLogger(TableCreator.class);

    private final TransportCreateTableAction transportCreateTableAction;

    @Inject
    public TableCreator(TransportCreateTableAction transportCreateIndexAction) {
        this.transportCreateTableAction = transportCreateIndexAction;
    }

    public CompletableFuture<Long> create(BoundCreateTable createTable, Version minNodeVersion) {
        var templateName = createTable.templateName();
        var relationName = createTable.tableIdent();
        CreateTableRequest createTableRequest;

        LinkedHashMap<ColumnIdent, Reference> references = new LinkedHashMap<>();
        IntArrayList pKeysIndices = new IntArrayList();
        createTable.analyzedTableElements().collectReferences(relationName, references, pKeysIndices, true);
        var policy = (String) createTable.tableParameter().mappings().get(ColumnPolicies.ES_MAPPING_NAME);
        var tableColumnPolicy = policy != null ? ColumnPolicies.decodeMappingValue(policy) : ColumnPolicy.STRICT;

        if (minNodeVersion.onOrAfter(Version.V_5_4_0)) {
            createTableRequest = new CreateTableRequest(
                relationName,
                new ArrayList<>(references.values()),
                pKeysIndices,
                createTable.analyzedTableElements().getCheckConstraints(),
                createTable.tableParameter().settings(),
                createTable.routingColumn(),
                tableColumnPolicy,
                createTable.partitionedBy()
            );
        } else {
            // TODO: Remove BWC branch in 5.5.
            var mapping = createMapping(
                MappingUtil.AllocPosition.forNewTable(),
                new ArrayList<>(references.values()),
                pKeysIndices,
                createTable.analyzedTableElements().getCheckConstraints(),
                createTable.partitionedBy(),
                tableColumnPolicy,
                createTable.routingColumn()
            );
            createTableRequest = templateName == null
                ? new CreateTableRequest(
                    new CreateIndexRequest(
                        relationName.indexNameOrAlias(),
                        createTable.tableParameter().settings()
                    ).mapping(mapping)
            )
                : new CreateTableRequest(
                    new PutIndexTemplateRequest(templateName)
                        .mapping(mapping)
                        .create(true)
                        .settings(createTable.tableParameter().settings())
                        .patterns(Collections.singletonList(createTable.templatePrefix()))
                        .alias(new Alias(relationName.indexNameOrAlias()))
            );
        }
        return transportCreateTableAction.execute(createTableRequest, resp -> {
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
                return Exceptions.rethrowRuntimeException(cause);
            } else if (createTable.ifNotExists() && isTableExistsError(t, templateName)) {
                return 0L;
            } else {
                return Exceptions.rethrowRuntimeException(t);
            }
        });
    }

    private static boolean isTableExistsError(Throwable e, @Nullable String templateName) {
        return e instanceof ResourceAlreadyExistsException
               || (templateName != null && isTemplateAlreadyExistsException(e));
    }

    private static boolean isTemplateAlreadyExistsException(Throwable e) {
        return e instanceof IllegalArgumentException
            && e.getMessage() != null && e.getMessage().endsWith("already exists");
    }
}

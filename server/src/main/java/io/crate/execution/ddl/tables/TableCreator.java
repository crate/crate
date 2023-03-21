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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import io.crate.Constants;
import io.crate.planner.PlannerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.analyze.BoundCreateTable;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;
import org.elasticsearch.common.xcontent.XContentFactory;

@Singleton
public class TableCreator {

    protected static final Logger LOGGER = LogManager.getLogger(TableCreator.class);

    private final TransportCreateTableAction transportCreateTableAction;
    private final PlannerContext plannerContext;

    @Inject
    public TableCreator(TransportCreateTableAction transportCreateIndexAction, PlannerContext plannerContext) {
        this.transportCreateTableAction = transportCreateIndexAction;
        this.plannerContext = plannerContext;
    }

    public CompletableFuture<Long> create(BoundCreateTable createTable) throws IOException {
        var templateName = createTable.templateName();
        var relationName = createTable.tableIdent();
        CreateTableRequest createTableRequest;
        if (plannerContext.clusterState().nodes().getMinNodeVersion().onOrAfter(Version.V_5_3_0)) {
            var map = createTable.mapping();
            // Wrap partitioned table mapping in a type map to align with PutIndexTemplateRequest.mapping()
            if (templateName != null) {
                if (map.size() != 1 || !map.containsKey(Constants.DEFAULT_MAPPING_TYPE)) {
                    map = Map.of(Constants.DEFAULT_MAPPING_TYPE, map);
                }
            }
            String mapping = Strings.toString(XContentFactory.jsonBuilder().map(map));
            createTableRequest = new CreateTableRequest(relationName, createTable.tableParameter().settings(), mapping, templateName != null);
        } else {
            // TODO: Remove this in 5.4 to have a single entry point to assign column OID-s on a table creation.
            createTableRequest = templateName == null
                ? new CreateTableRequest(
                new CreateIndexRequest(
                    relationName.indexNameOrAlias(),
                    createTable.tableParameter().settings()
                ).mapping(createTable.mapping())
            )
                : new CreateTableRequest(
                new PutIndexTemplateRequest(templateName)
                    .mapping(createTable.mapping())
                    .create(true)
                    .settings(createTable.tableParameter().settings())
                    .patterns(Collections.singletonList(createTable.templatePrefix()))
                    .order(100)
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

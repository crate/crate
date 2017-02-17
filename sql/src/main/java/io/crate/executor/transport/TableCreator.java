/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport;

import com.google.common.base.Joiner;
import io.crate.Constants;
import io.crate.analyze.CreateTableAnalyzedStatement;
import io.crate.exceptions.SQLExceptions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexTemplateAlreadyExistsException;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;

@Singleton
public class TableCreator {

    private static final Long SUCCESS_RESULT = 1L;

    protected static final ESLogger logger = Loggers.getLogger(TableCreator.class);

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final TransportActionProvider transportActionProvider;

    @Inject
    public TableCreator(ClusterService clusterService,
                        IndexNameExpressionResolver indexNameExpressionResolver,
                        TransportActionProvider transportActionProvider) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.transportActionProvider = transportActionProvider;
    }


    public CompletableFuture<Long> create(CreateTableAnalyzedStatement statement) {
        final CompletableFuture<Long> result = new CompletableFuture<>();

        // real work done in createTable()
        deleteOrphans(new CreateTableResponseListener(result, statement), statement);
        return result;
    }

    private CreateIndexRequest createIndexRequest(CreateTableAnalyzedStatement statement) {
        return new CreateIndexRequest(statement.tableIdent().indexName(), settings(statement))
            .mapping(Constants.DEFAULT_MAPPING_TYPE, statement.mapping());
    }

    private Settings settings(CreateTableAnalyzedStatement statement) {
        return statement.tableParameter().settings().getByPrefix("index.");
    }

    private PutIndexTemplateRequest createTemplateRequest(CreateTableAnalyzedStatement statement) {
        return new PutIndexTemplateRequest(statement.templateName())
            .mapping(Constants.DEFAULT_MAPPING_TYPE, statement.mapping())
            .create(true)
            .settings(settings(statement))
            .template(statement.templatePrefix())
            .order(100)
            .alias(new Alias(statement.tableIdent().indexName()));
    }

    private void createTable(final CompletableFuture<Long> result, final CreateTableAnalyzedStatement statement) {
        if (statement.templateName() != null) {
            transportActionProvider.transportPutIndexTemplateAction().execute(createTemplateRequest(statement), new ActionListener<PutIndexTemplateResponse>() {
                @Override
                public void onResponse(PutIndexTemplateResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged(String.format(Locale.ENGLISH, "creating table '%s'", statement.tableIdent().fqn()));
                    }
                    result.complete(SUCCESS_RESULT);
                }

                @Override
                public void onFailure(Throwable e) {
                    setException(result, e, statement);
                }
            });
        } else {
            transportActionProvider.transportCreateIndexAction().execute(createIndexRequest(statement), new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged(String.format(Locale.ENGLISH, "creating table '%s'", statement.tableIdent().fqn()));
                    }
                    result.complete(SUCCESS_RESULT);
                }

                @Override
                public void onFailure(Throwable e) {
                    setException(result, e, statement);
                }
            });
        }
    }


    private void setException(CompletableFuture<Long> result, Throwable e, CreateTableAnalyzedStatement statement) {
        e = SQLExceptions.unwrap(e);
        String message = e.getMessage();
        if ("mapping [default]".equals(message) && e.getCause() != null) {
            // this is a generic mapping parse exception,
            // the cause has usually a better more detailed error message
            result.completeExceptionally(e.getCause());
        } else if (statement.ifNotExists() &&
                   (e instanceof IndexAlreadyExistsException
                    || (e instanceof IndexTemplateAlreadyExistsException && statement.templateName() != null))) {
            result.complete(null);
        } else {
            result.completeExceptionally(e);
        }
    }

    private void deleteOrphans(final CreateTableResponseListener listener, final CreateTableAnalyzedStatement statement) {
        if (clusterService.state().metaData().hasAlias(statement.tableIdent().fqn())
            && PartitionName.isPartition(
            clusterService.state().metaData().getAliasAndIndexLookup().get(statement.tableIdent().fqn()).getIndices().iterator().next().getIndex())) {
            logger.debug("Deleting orphaned partitions with alias: {}", statement.tableIdent().fqn());
            transportActionProvider.transportDeleteIndexAction().execute(new DeleteIndexRequest(statement.tableIdent().fqn()), new ActionListener<DeleteIndexResponse>() {
                @Override
                public void onResponse(DeleteIndexResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged("deleting orphaned alias");
                    }
                    deleteOrphanedPartitions(listener, statement.tableIdent());
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } else {
            deleteOrphanedPartitions(listener, statement.tableIdent());
        }
    }

    /**
     * if some orphaned partition with the same table name still exist,
     * delete them beforehand as they would create unwanted and maybe invalid
     * initial data.
     * <p>
     * should never delete partitions of existing partitioned tables
     */
    private void deleteOrphanedPartitions(final CreateTableResponseListener listener, TableIdent tableIdent) {
        String partitionWildCard = PartitionName.templateName(tableIdent.schema(), tableIdent.name()) + "*";
        String[] orphans = indexNameExpressionResolver.concreteIndices(clusterService.state(), IndicesOptions.strictExpand(), partitionWildCard);
        if (orphans.length > 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Deleting orphaned partitions: {}", Joiner.on(", ").join(orphans));
            }
            transportActionProvider.transportDeleteIndexAction().execute(new DeleteIndexRequest(orphans), new ActionListener<DeleteIndexResponse>() {
                @Override
                public void onResponse(DeleteIndexResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged("deleting orphans");
                    }
                    listener.onResponse(SUCCESS_RESULT);
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } else {
            listener.onResponse(SUCCESS_RESULT);
        }
    }

    protected void warnNotAcknowledged(String operationName) {
        logger.warn("{} was not acknowledged. This could lead to inconsistent state.",
            operationName);
    }

    class CreateTableResponseListener implements ActionListener<Long> {

        final CompletableFuture<Long> result;
        final CreateTableAnalyzedStatement statement;

        public CreateTableResponseListener(CompletableFuture<Long> result, CreateTableAnalyzedStatement statement) {
            this.result = result;
            this.statement = statement;

        }

        @Override
        public void onResponse(Long ignored) {
            createTable(result, statement);
        }

        @Override
        public void onFailure(Throwable e) {
            result.completeExceptionally(e);
        }
    }
}

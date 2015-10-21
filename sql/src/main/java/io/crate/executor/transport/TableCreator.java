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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.analyze.CreateTableAnalyzedStatement;
import io.crate.exceptions.Exceptions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexTemplateAlreadyExistsException;

import java.util.Locale;

public class TableCreator {

    protected static final ESLogger logger = Loggers.getLogger(TableCreator.class);

    private final ClusterService clusterService;
    private final TransportCreateIndexAction createIndexAction;
    private final TransportDeleteIndexAction deleteIndexAction;
    private final TransportPutIndexTemplateAction putIndexTemplateAction;
    private final CreateTableAnalyzedStatement statement;

    private final Settings settings;

    public TableCreator(ClusterService clusterService,
                        TransportCreateIndexAction createIndexAction,
                        TransportDeleteIndexAction deleteIndexAction,
                        TransportPutIndexTemplateAction putIndexTemplateAction,
                        CreateTableAnalyzedStatement statement) {
        this.clusterService = clusterService;
        this.createIndexAction = createIndexAction;
        this.deleteIndexAction = deleteIndexAction;
        this.putIndexTemplateAction = putIndexTemplateAction;
        this.statement = statement;

        this.settings = statement.tableParameter().settings().getByPrefix("index.");
    }


    public ListenableFuture<Void> start() {
        final SettableFuture<Void> result = SettableFuture.create();

        // real work done in createTable()
        deleteOrphans(new CreateTableResponseListener(result));
        return result;
    }

    private CreateIndexRequest createIndexRequest() {
        return new CreateIndexRequest(statement.tableIdent().indexName(), settings)
                .mapping(Constants.DEFAULT_MAPPING_TYPE, statement.mapping());
    }

    private PutIndexTemplateRequest createTemplateRequest() {
        return new PutIndexTemplateRequest(statement.templateName())
                .mapping(Constants.DEFAULT_MAPPING_TYPE, statement.mapping())
                .create(true)
                .settings(settings)
                .template(statement.templatePrefix())
                .order(100)
                .alias(new Alias(statement.tableIdent().indexName()));
    }

    private void createTable(final SettableFuture<Void> result) {
        if (statement.templateName() != null) {
            putIndexTemplateAction.execute(createTemplateRequest(), new ActionListener<PutIndexTemplateResponse>() {
                @Override
                public void onResponse(PutIndexTemplateResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged(String.format(Locale.ENGLISH, "creating table '%s'", statement.tableIdent().fqn()));
                    }
                    result.set(null);
                }

                @Override
                public void onFailure(Throwable e) {
                    setException(result, e);
                }
            });
        } else {
            createIndexAction.execute(createIndexRequest(), new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged(String.format(Locale.ENGLISH, "creating table '%s'", statement.tableIdent().fqn()));
                    }
                    result.set(null);
                }

                @Override
                public void onFailure(Throwable e) {
                    setException(result, e);
                }
            });
        }
    }


    private void setException(SettableFuture<Void> result, Throwable e) {
        e = Exceptions.unwrap(e);
        String message = e.getMessage();
        if (message.equals("mapping [default]") && e.getCause() != null) {
            // this is a generic mapping parse exception,
            // the cause has usually a better more detailed error message
            result.setException(e.getCause());
        } else if (statement.ifNotExists() &&
                   (e instanceof IndexAlreadyExistsException
                    || (e instanceof IndexTemplateAlreadyExistsException && statement.templateName() != null))) {
            result.set(null);
        } else {
            result.setException(e);
        }
    }

    private void deleteOrphans(final CreateTableResponseListener listener) {
        if (clusterService.state().metaData().aliases().containsKey(statement.tableIdent().fqn())
            && PartitionName.isPartition(
                clusterService.state().metaData().aliases().get(statement.tableIdent().fqn()).keysIt().next())) {
            logger.debug("Deleting orphaned partitions with alias: {}", statement.tableIdent().fqn());
            deleteIndexAction.execute(new DeleteIndexRequest(statement.tableIdent().fqn()), new ActionListener<DeleteIndexResponse>() {
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
     *
     * should never delete partitions of existing partitioned tables
     */
    private void deleteOrphanedPartitions(final CreateTableResponseListener listener, TableIdent tableIdent) {
        String partitionWildCard = PartitionName.templateName(tableIdent.schema(), tableIdent.name()) + "*";
        String[] orphans = clusterService.state().metaData().concreteIndices(
                new String[]{partitionWildCard});
        if (orphans.length > 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Deleting orphaned partitions: {}", Joiner.on(", ").join(orphans));
            }
            deleteIndexAction.execute(new DeleteIndexRequest(orphans), new ActionListener<DeleteIndexResponse>() {
                @Override
                public void onResponse(DeleteIndexResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged("deleting orphans");
                    }
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } else {
            listener.onResponse(null);
        }
    }

    protected void warnNotAcknowledged(String operationName) {
        logger.warn("{} was not acknowledged. This could lead to inconsistent state.",
                operationName);
    }

    class CreateTableResponseListener implements ActionListener<Void> {

        final SettableFuture<Void> result;

        public CreateTableResponseListener(SettableFuture<Void> result) {
            this.result = result;

        }

        @Override
        public void onResponse(Void ignored) {
            createTable(result);
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }
}

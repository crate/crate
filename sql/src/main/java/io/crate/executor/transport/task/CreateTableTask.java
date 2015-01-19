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

package io.crate.executor.transport.task;

import com.google.common.base.Joiner;
import io.crate.Constants;
import io.crate.executor.TaskResult;
import io.crate.metadata.PartitionName;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.TaskExecutionException;
import io.crate.planner.node.ddl.CreateTableNode;
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

import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class CreateTableTask extends AbstractChainedTask {

    private static final TaskResult SUCCESS_RESULT = TaskResult.ONE_ROW;

    private final ClusterService clusterService;
    private final TransportCreateIndexAction createIndexAction;
    private final TransportDeleteIndexAction deleteIndexAction;
    private final TransportPutIndexTemplateAction putIndexTemplateAction;
    private final CreateTableNode planNode;

    public CreateTableTask(UUID jobId,
                           ClusterService clusterService,
                           TransportCreateIndexAction createIndexAction,
                           TransportDeleteIndexAction deleteIndexAction, TransportPutIndexTemplateAction putIndexTemplateAction,
                           CreateTableNode node) {
        super(jobId);
        this.clusterService = clusterService;
        this.createIndexAction = createIndexAction;
        this.deleteIndexAction = deleteIndexAction;
        this.putIndexTemplateAction = putIndexTemplateAction;
        this.planNode = node;
    }

    @Override
    protected void doStart(List<TaskResult> upstreamResults) {
        // real work done in createTable()
        deleteOrphans(new CreateTableResponseListener());
    }

    private CreateIndexRequest createIndexRequest() {
        return new CreateIndexRequest(planNode.tableIdent().esName(), planNode.settings())
                .mapping(Constants.DEFAULT_MAPPING_TYPE, planNode.mapping());
    }

    private PutIndexTemplateRequest createTemplateRequest() {
        return new PutIndexTemplateRequest(planNode.templateName().get())
                .mapping(Constants.DEFAULT_MAPPING_TYPE, planNode.mapping())
                .create(true)
                .settings(planNode.settings())
                .template(planNode.templateIndexMatch().get())
                .alias(new Alias(planNode.tableIdent().esName()));
    }

    private void createTable() {
        if (planNode.createsPartitionedTable()) {
            putIndexTemplateAction.execute(createTemplateRequest(), new ActionListener<PutIndexTemplateResponse>() {
                @Override
                public void onResponse(PutIndexTemplateResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged(String.format(Locale.ENGLISH, "creating table '%s'", planNode.tableIdent().fqn()));
                    }
                    result.set(SUCCESS_RESULT);
                }

                @Override
                public void onFailure(Throwable e) {
                    setException(e);
                }
            });
        } else {
            createIndexAction.execute(createIndexRequest(), new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged(String.format(Locale.ENGLISH, "creating table '%s'", planNode.tableIdent().fqn()));
                    }
                    result.set(SUCCESS_RESULT);
                }

                @Override
                public void onFailure(Throwable e) {
                    setException(e);
                }
            });
        }
    }

    private void setException(Throwable e) {
        e = Exceptions.unwrap(e);
        String message = e.getMessage();
        if (message.equals("mapping [default]") && e.getCause() != null) {
            // this is a generic mapping parse exception,
            // the cause has usually a better more detailed error message
            result.setException(e.getCause());
        } else {
            result.setException(e);
        }
    }


    private void deleteOrphans(final CreateTableResponseListener listener) {
        if (clusterService.state().metaData().aliases().containsKey(planNode.tableIdent().fqn())
                && PartitionName.isPartition(
                clusterService.state().metaData().aliases().get(planNode.tableIdent().fqn()).keysIt().next(),
                planNode.tableIdent().schema(),
                planNode.tableIdent().name())
                ) {
            logger.debug("Deleting orphaned partitions with alias: {}", planNode.tableIdent().fqn());
            deleteIndexAction.execute(new DeleteIndexRequest(planNode.tableIdent().fqn()), new ActionListener<DeleteIndexResponse>() {
                @Override
                public void onResponse(DeleteIndexResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged("deleting orphaned alias");
                    }
                    deleteOrphanedPartitions(listener);
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } else {
            deleteOrphanedPartitions(listener);
        }
    }

    /**
     * if some orphaned partition with the same table name still exist,
     * delete them beforehand as they would create unwanted and maybe invalid
     * initial data.
     *
     * should never delete partitions of existing partitioned tables
     */
    private void deleteOrphanedPartitions(final CreateTableResponseListener listener) {
        String partitionWildCard = PartitionName.templateName(planNode.tableIdent().schema(), planNode.tableIdent().name()) + "*";
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

    class CreateTableResponseListener implements ActionListener<Void> {

        @Override
        public void onResponse(Void ignored) {
            createTable();
        }

        @Override
        public void onFailure(Throwable e) {
            throw new TaskExecutionException(CreateTableTask.this, e);
        }
    }

}


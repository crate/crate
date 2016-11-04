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

import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.executor.JobTask;
import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.RowReceivers;
import io.crate.planner.node.ddl.DropTablePlan;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexTemplateMissingException;

public class DropTableTask extends JobTask {

    private static final Row ROW_ZERO = new Row1(0L);
    private static final Row ROW_ONE = new Row1(1L);

    private static final Logger logger = Loggers.getLogger(DropTableTask.class);

    private final DocTableInfo tableInfo;
    private final TransportDeleteIndexTemplateAction deleteTemplateAction;
    private final TransportDeleteIndexAction deleteIndexAction;
    private final boolean ifExists;

    public DropTableTask(DropTablePlan plan,
                         TransportDeleteIndexTemplateAction deleteTemplateAction,
                         TransportDeleteIndexAction deleteIndexAction) {
        super(plan.jobId());
        this.ifExists = plan.ifExists();
        this.tableInfo = plan.tableInfo();
        this.deleteTemplateAction = deleteTemplateAction;
        this.deleteIndexAction = deleteIndexAction;
    }

    @Override
    public void execute(final RowReceiver rowReceiver, Row parameters) {
        if (tableInfo.isPartitioned()) {
            String templateName = PartitionName.templateName(tableInfo.ident().schema(), tableInfo.ident().name());
            deleteTemplateAction.execute(new DeleteIndexTemplateRequest(templateName), new ActionListener<DeleteIndexTemplateResponse>() {
                @Override
                public void onResponse(DeleteIndexTemplateResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged();
                    }
                    if (!tableInfo.partitions().isEmpty()) {
                        deleteESIndex(tableInfo.ident().indexName(), rowReceiver);
                    } else {
                        RowReceivers.sendOneRow(rowReceiver, ROW_ONE);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    Throwable t = ExceptionsHelper.unwrapCause(e);
                    if (t instanceof IndexTemplateMissingException && !tableInfo.partitions().isEmpty()) {
                        logger.warn(t.getMessage());
                        deleteESIndex(tableInfo.ident().indexName(), rowReceiver);
                    } else {
                        rowReceiver.fail(t);
                    }
                }
            });
        } else {
            deleteESIndex(tableInfo.ident().indexName(), rowReceiver);
        }
    }

    private void deleteESIndex(String indexOrAlias, final RowReceiver rowReceiver) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexOrAlias);
        if (tableInfo.isPartitioned()) {
            deleteIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        }
        deleteIndexAction.execute(deleteIndexRequest, new ActionListener<DeleteIndexResponse>() {
            @Override
            public void onResponse(DeleteIndexResponse response) {
                if (!response.isAcknowledged()) {
                    warnNotAcknowledged();
                }
                RowReceivers.sendOneRow(rowReceiver, ROW_ONE);
            }

            @Override
            public void onFailure(Exception e) {
                if (tableInfo.isPartitioned()) {
                    logger.warn("Could not (fully) delete all partitions of {}. " +
                                "Some orphaned partitions might still exist, " +
                                "but are not accessible.", e, tableInfo.ident().fqn());
                }
                if (ifExists && e instanceof IndexNotFoundException) {
                    RowReceivers.sendOneRow(rowReceiver, ROW_ZERO);
                } else {
                    rowReceiver.fail(e);
                }
            }
        });
    }

    private void warnNotAcknowledged() {
        if (logger.isWarnEnabled()) {
            logger.warn("Dropping table {} was not acknowledged. This could lead to inconsistent state.", tableInfo.ident());
        }
    }
}

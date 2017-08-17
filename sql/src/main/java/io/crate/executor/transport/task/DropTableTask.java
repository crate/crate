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

import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.executor.JobTask;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.user.UserManager;
import io.crate.planner.node.ddl.DropTablePlan;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexTemplateMissingException;

import java.util.Locale;

import static io.crate.data.SentinelRow.SENTINEL;

public class DropTableTask extends JobTask {

    private static final Row ROW_ZERO = new Row1(0L);
    private static final Row ROW_ONE = new Row1(1L);

    private static final Logger logger = Loggers.getLogger(DropTableTask.class);

    private final DocTableInfo tableInfo;
    private final TransportDeleteIndexTemplateAction deleteTemplateAction;
    private final TransportDeleteIndexAction deleteIndexAction;
    private final UserManager userManager;
    private final boolean ifExists;

    public DropTableTask(DropTablePlan plan,
                         TransportDeleteIndexTemplateAction deleteTemplateAction,
                         TransportDeleteIndexAction deleteIndexAction,
                         UserManager userManager) {
        super(plan.jobId());
        this.ifExists = plan.ifExists();
        this.tableInfo = plan.tableInfo();
        this.deleteTemplateAction = deleteTemplateAction;
        this.deleteIndexAction = deleteIndexAction;
        this.userManager = userManager;
    }

    @Override
    public void execute(final RowConsumer consumer, Row parameters) {
        if (tableInfo.isPartitioned()) {
            String templateName = PartitionName.templateName(tableInfo.ident().schema(), tableInfo.ident().name());
            deleteTemplateAction.execute(new DeleteIndexTemplateRequest(templateName), new ActionListener<DeleteIndexTemplateResponse>() {
                @Override
                public void onResponse(DeleteIndexTemplateResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged();
                    }
                    if (!tableInfo.partitions().isEmpty()) {
                        deleteESIndex(tableInfo.ident(), consumer);
                    } else {
                        userManager.dropTablePrivileges(tableInfo.ident().fqn()).whenComplete((r, t) -> {
                            if (t != null) {
                                logger.warn(
                                    String.format(Locale.ENGLISH, "Unable to drop existing privileges for table %s.", tableInfo.ident().fqn()),
                                    t);
                            }
                            consumer.accept(InMemoryBatchIterator.of(ROW_ONE, SENTINEL), null);
                        });
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    Throwable t = ExceptionsHelper.unwrapCause(e);
                    if (t instanceof IndexTemplateMissingException && !tableInfo.partitions().isEmpty()) {
                        logger.warn(t.getMessage());
                        deleteESIndex(tableInfo.ident(), consumer);
                    } else {
                        consumer.accept(null, t);
                    }
                }
            });
        } else {
            deleteESIndex(tableInfo.ident(), consumer);
        }
    }

    private void deleteESIndex(TableIdent tableIdent, final RowConsumer consumer) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(tableIdent.indexName());
        if (tableInfo.isPartitioned()) {
            deleteIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        }
        deleteIndexAction.execute(deleteIndexRequest, new ActionListener<DeleteIndexResponse>() {
            @Override
            public void onResponse(DeleteIndexResponse response) {
                if (!response.isAcknowledged()) {
                    warnNotAcknowledged();
                }
                userManager.dropTablePrivileges(tableIdent.fqn()).whenComplete((r, t) -> {
                    if (t != null) {
                        logger.warn(
                            String.format(Locale.ENGLISH, "Unable to drop existing privileges for table %s.", tableIdent.fqn()),
                            t);
                    }
                    consumer.accept(InMemoryBatchIterator.of(ROW_ONE, SENTINEL), null);
                });
            }

            @Override
            public void onFailure(Exception e) {
                if (tableInfo.isPartitioned()) {
                    logger.warn(
                        String.format(Locale.ENGLISH, "Could not (fully) delete all partitions of %s. " +
                                                      "Some orphaned partitions might still exist, " +
                                                      "but are not accessible.", tableInfo.ident().fqn()),
                        e);
                }
                if (ifExists && e instanceof IndexNotFoundException) {
                    consumer.accept(InMemoryBatchIterator.of(ROW_ZERO, SENTINEL), null);
                } else {
                    consumer.accept(null, e);
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

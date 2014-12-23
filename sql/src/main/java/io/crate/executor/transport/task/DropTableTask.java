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

import io.crate.executor.TaskResult;
import io.crate.metadata.PartitionName;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.node.ddl.DropTableNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class DropTableTask extends AbstractChainedTask {

    private static final TaskResult SUCCESS_RESULT = TaskResult.ONE_ROW;

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final TableInfo tableInfo;
    private final TransportDeleteIndexTemplateAction deleteTemplateAction;
    private final TransportDeleteIndexAction deleteIndexAction;

    public DropTableTask(UUID jobId,
                         TransportDeleteIndexTemplateAction deleteTemplateAction,
                         TransportDeleteIndexAction deleteIndexAction,
                         DropTableNode node) {
        super(jobId);
        this.tableInfo = node.tableInfo();
        this.deleteTemplateAction = deleteTemplateAction;
        this.deleteIndexAction = deleteIndexAction;
    }


    @Override
    protected void doStart(List<TaskResult> upstreamResults) {
        if (tableInfo.isPartitioned()) {
            String templateName = PartitionName.templateName(tableInfo.ident().schema(), tableInfo.ident().name());
            deleteTemplateAction.execute(new DeleteIndexTemplateRequest(templateName), new ActionListener<DeleteIndexTemplateResponse>() {
                @Override
                public void onResponse(DeleteIndexTemplateResponse response) {
                    if (!response.isAcknowledged()) {
                        warnNotAcknowledged(String.format(Locale.ENGLISH, "dropping table '%s'", tableInfo.ident().fqn()));
                    }
                    if (!tableInfo.partitions().isEmpty()) {
                        deleteESIndex(tableInfo.ident().esName());
                    } else {
                        result.set(SUCCESS_RESULT);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    result.setException(e);
                }
            });
        } else {
            deleteESIndex(tableInfo.ident().esName());
        }

    }

    private void deleteESIndex(String indexOrAlias) {
        deleteIndexAction.execute(new DeleteIndexRequest(indexOrAlias), new ActionListener<DeleteIndexResponse>() {
            @Override
            public void onResponse(DeleteIndexResponse response) {
                if (!response.isAcknowledged()) {
                    warnNotAcknowledged(String.format(Locale.ENGLISH, "dropping table '%s'", tableInfo.ident().fqn()));
                }
                result.set(SUCCESS_RESULT);
            }

            @Override
            public void onFailure(Throwable e) {
                if (tableInfo.isPartitioned()) {
                    logger.warn("Could not (fully) delete all partitions of {}. " +
                            "Some orphaned partitions might still exist, " +
                            "but are not accessible.", e, tableInfo.ident().fqn());
                }
                result.setException(e);
            }
        });
    }
}

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

import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowsBatchIterator;
import io.crate.executor.JobTask;
import io.crate.executor.transport.ddl.DropTableRequest;
import io.crate.executor.transport.ddl.DropTableResponse;
import io.crate.executor.transport.ddl.TransportDropTableAction;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.node.ddl.DropTablePlan;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Locale;

public class DropTableTask extends JobTask {

    private static final Row ROW_ZERO = new Row1(0L);
    private static final Row ROW_ONE = new Row1(1L);

    private static final Logger LOGGER = Loggers.getLogger(DropTableTask.class);

    private final DocTableInfo tableInfo;
    private final TransportDropTableAction transportDropTableAction;
    private final boolean ifExists;

    public DropTableTask(DropTablePlan plan,
                         TransportDropTableAction transportDropTableAction) {
        super(plan.jobId());
        this.ifExists = plan.ifExists();
        this.tableInfo = plan.tableInfo();
        this.transportDropTableAction = transportDropTableAction;
    }

    @Override
    public void execute(final BatchConsumer consumer, Row parameters) {
        TableIdent tableIdent = tableInfo.ident();
        DropTableRequest request = new DropTableRequest(tableIdent, tableInfo.isPartitioned());
        transportDropTableAction.execute(request, new ActionListener<DropTableResponse>() {
            @Override
            public void onResponse(DropTableResponse response) {
                if (!response.isAcknowledged()) {
                    warnNotAcknowledged();
                }
                consumer.accept(RowsBatchIterator.newInstance(ROW_ONE), null);
            }

            @Override
            public void onFailure(Exception e) {
                if (tableInfo.isPartitioned()) {
                    LOGGER.warn(
                        String.format(Locale.ENGLISH, "Could not (fully) delete all partitions of %s. " +
                                                      "Some orphaned partitions might still exist, " +
                                                      "but are not accessible.", tableInfo.ident().fqn()),
                        e);
                }
                if (ifExists && e instanceof IndexNotFoundException) {
                    consumer.accept(RowsBatchIterator.newInstance(ROW_ZERO), null);
                } else {
                    consumer.accept(null, e);
                }
            }
        });
    }

    private void warnNotAcknowledged() {
        if (LOGGER.isWarnEnabled()) {
            LOGGER.warn("Dropping table {} was not acknowledged. This could lead to inconsistent state.", tableInfo.ident());
        }
    }
}

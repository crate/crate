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

package io.crate.planner.node.ddl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshot;

import io.crate.analyze.AnalyzedDropSnapshot;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;

public class DropSnapshotPlan implements Plan {

    private static final Logger LOGGER = LogManager.getLogger(DropSnapshotPlan.class);

    private final AnalyzedDropSnapshot dropSnapshot;

    public DropSnapshotPlan(AnalyzedDropSnapshot dropSnapshot) {
        this.dropSnapshot = dropSnapshot;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row parameters,
                              SubQueryResults subQueryResult) {
        DeleteSnapshotRequest request = new DeleteSnapshotRequest(
            dropSnapshot.repository(), dropSnapshot.snapshot());

        dependencies.client().execute(TransportDeleteSnapshot.ACTION, request)
            .whenComplete(new OneRowActionListener<>(
                consumer,
                response -> {
                    if (!response.isAcknowledged()) {
                        LOGGER.info(
                            "delete snapshots '{}.[{}]' not acknowledged",
                            request.repository(), request.snapshots());
                    }
                    return new Row1(1L);
                }));
    }
}

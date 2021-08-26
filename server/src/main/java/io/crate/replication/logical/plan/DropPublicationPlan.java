/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.replication.logical.plan;

import io.crate.replication.logical.analyze.AnalyzedDropPublication;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.replication.logical.action.DropPublicationRequest;
import io.crate.execution.support.OneRowActionListener;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;

public class DropPublicationPlan implements Plan {

    private final AnalyzedDropPublication analyzedDropPublication;

    public DropPublicationPlan(AnalyzedDropPublication analyzedDropPublication) {
        this.analyzedDropPublication = analyzedDropPublication;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params, SubQueryResults subQueryResults) throws Exception {
        var request = new DropPublicationRequest(analyzedDropPublication.name(), analyzedDropPublication.ifExists());
        dependencies.dropPublicationAction()
            .execute(request, new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : 1L)));
    }
}

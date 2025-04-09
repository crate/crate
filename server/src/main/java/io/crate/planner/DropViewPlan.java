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

package io.crate.planner;

import java.util.function.Function;

import io.crate.analyze.AnalyzedDropView;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.exceptions.RelationsUnknown;
import io.crate.execution.ddl.views.DropViewRequest;
import io.crate.execution.ddl.views.DropViewResponse;
import io.crate.execution.ddl.views.TransportDropView;
import io.crate.execution.support.OneRowActionListener;
import io.crate.planner.operators.SubQueryResults;

public class DropViewPlan implements Plan {

    private final AnalyzedDropView dropView;

    DropViewPlan(AnalyzedDropView dropView) {
        this.dropView = dropView;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        DropViewRequest request = new DropViewRequest(dropView.views(), dropView.ifExists());
        Function<DropViewResponse, Row> responseToRow = resp -> {
            if (dropView.ifExists() || resp.missing().isEmpty()) {
                return new Row1((long) dropView.views().size() - resp.missing().size());
            }
            throw new RelationsUnknown(resp.missing());
        };
        dependencies.client().execute(TransportDropView.ACTION, request)
            .whenComplete(new OneRowActionListener<>(consumer, responseToRow));
    }
}

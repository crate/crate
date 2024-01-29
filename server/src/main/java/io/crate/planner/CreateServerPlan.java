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

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import io.crate.analyze.AnalyzedCreateServer;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.fdw.CreateServerRequest;
import io.crate.fdw.TransportCreateServerAction;
import io.crate.planner.operators.SubQueryAndParamBinder;
import io.crate.planner.operators.SubQueryResults;

public class CreateServerPlan implements Plan {

    private final AnalyzedCreateServer createServer;

    public CreateServerPlan(AnalyzedCreateServer createServer) {
        this.createServer = createServer;
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
                              SubQueryResults subQueryResults) throws Exception {

        var subQueryAndParamBinder = new SubQueryAndParamBinder(params, subQueryResults);
        Map<String, Object> options = createServer.options().entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, entry -> subQueryAndParamBinder.apply(entry.getValue())));
        CreateServerRequest request = new CreateServerRequest(
            createServer.name(),
            createServer.fdw(),
            createServer.ifNotExists(),
            options
        );
        dependencies.client()
            .execute(TransportCreateServerAction.ACTION, request)
            .whenComplete(OneRowActionListener.oneIfAcknowledged(consumer));
    }
}

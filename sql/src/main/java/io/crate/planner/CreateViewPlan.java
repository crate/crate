/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner;

import io.crate.analyze.CreateViewStmt;
import io.crate.auth.user.User;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.execution.ddl.views.CreateViewRequest;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.SelectSymbol;

import java.util.Map;

public final class CreateViewPlan implements Plan {

    private final CreateViewStmt createViewStmt;

    CreateViewPlan(CreateViewStmt createViewStmt) {
        this.createViewStmt = createViewStmt;
    }

    @Override
    public void execute(DependencyCarrier dependencies,
                        PlannerContext plannerContext,
                        RowConsumer consumer,
                        Row params,
                        Map<SelectSymbol, Object> valuesBySubQuery) {

        User owner = createViewStmt.owner();
        CreateViewRequest request = new CreateViewRequest(
            createViewStmt.name(),
            createViewStmt.formattedQuery(),
            createViewStmt.replaceExisting(),
            owner == null ? null : owner.name()
        );
        dependencies.createViewAction().execute(request, new OneRowActionListener<>(consumer, resp -> {
            if (resp.alreadyExistsFailure()) {
                throw new RelationAlreadyExists(createViewStmt.name());
            }
            return new Row1(1L);
        }));
    }
}

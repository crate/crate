/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node.management;

import io.crate.analyze.AbstractShowAnalyzedStatement;
import io.crate.planner.PlanVisitor;
import io.crate.planner.UnnestablePlan;

import java.util.UUID;

public class GenericShowPlan extends UnnestablePlan {

    private final UUID id;
    private final AbstractShowAnalyzedStatement statement;

    public GenericShowPlan(UUID jobId, AbstractShowAnalyzedStatement statement) {
        id = jobId;
        this.statement = statement;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitGenericShowPlan(this, context);
    }

    @Override
    public UUID jobId() {
        return id;
    }

    public AbstractShowAnalyzedStatement statement() {
        return statement;
    }
}

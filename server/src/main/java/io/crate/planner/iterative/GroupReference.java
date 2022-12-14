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

package io.crate.planner.iterative;

import java.util.List;

import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanId;
import io.crate.planner.operators.LogicalPlanVisitor;

public class GroupReference extends LogicalPlan
{
    private final int groupId;
    private final List<Symbol> outputs;
    private LogicalPlanId id;

    public GroupReference(LogicalPlanId id, int groupId, List<Symbol> outputs)
    {
        this.id = id;
        this.groupId = groupId;
        this.outputs = List.copyOf(outputs);
    }

    public int groupId()
    {
        return groupId;
    }

    @Override
    public List<LogicalPlan> sources()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupReference(this, context);
    }

    @Override
    public List<Symbol> outputs()
    {
        return outputs;
    }


}

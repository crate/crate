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

import io.crate.planner.distribution.DistributionInfo;
import io.crate.execution.dsl.projection.Projection;

import org.jspecify.annotations.Nullable;

public interface ExecutionPlan {

    <C, R> R accept(ExecutionPlanVisitor<C, R> visitor, C context);

    /**
     * Add a projection to the plan.
     */
    void addProjection(Projection projection);

    /**
     * Add a projection to the plan which changes the "unfinished" information on the ResultDescription.
     * For example, a projection which handles the limit, can change the `unfinishedLimit` of 10 to NO_LIMIT.
     */
    void addProjection(Projection projection,
                       int unfinishedLimit,
                       int unfinishedOffset,
                       @Nullable PositionalOrderBy unfinishedOrderBy);


    ResultDescription resultDescription();

    /**
     * Changes how the result will be distributed.
     */
    void setDistributionInfo(DistributionInfo distributionInfo);
}

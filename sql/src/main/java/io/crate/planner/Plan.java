/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
import java.util.UUID;

public interface Plan {

    <C, R> R accept(PlanVisitor<C, R> visitor, C context);

    UUID jobId();

    /**
     * Adds a projection to the plan.
     * The given values need to be used to update the ResultDescription.
     *
     * @param newLimit new limit if the projection is affecting the limit.
     * @param newOffset new offset if the projection is affecting the offset.
     * @param newOrderBy new orderBy if the projection is affecting the ordering.
     */
    void addProjection(Projection projection,
                       @Nullable Integer newLimit,
                       @Nullable Integer newOffset,
                       @Nullable PositionalOrderBy newOrderBy);

    ResultDescription resultDescription();

    /**
     * Changes how the result will be distributed.
     */
    void setDistributionInfo(DistributionInfo distributionInfo);
}

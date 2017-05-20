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

package io.crate.planner.consumer;

import io.crate.analyze.relations.QueriedRelation;
import io.crate.planner.Plan;

/**
 * <p>
 * A consumer is a component which can create a Plan for a relation.
 * A consumer plans as much work as possible but aims to keep the result distributed.
 *
 * It may also leave work specific to distribution "undone",
 * but then it must specify the remaining work to do in the ResultDescription
 * So that a parent-plan can easily pick it up.
 *
 * </p>
 * For example:
 *
 * <pre>
 *     SELECT count(*), name from t group by 2
 *
 *                  |
 *
 *      Plan:
 *          NODE1      NODE2
 *           CP         CP
 *            |__      /|
 *            |  \____/ |
 *            |_/    \__|
 *           MP         MP      <--- Merge Phase / Reduce Phase
 *                               Result is "complete" here - so a consumer should stop planning here
 * </pre>
 *
 * Or:
 *
 * <pre>
 *     SELECT * from t        -- no limit
 *
 *     Plan:
 *         NODE1    NODE2
 *           CP       CP          <-- Result is complete
 *
 * BUT:
 *
 *     SELECT * from t limit 10
 *
 *     Plan:
 *         NODE1     NODE2
 *           CP        CP         <-- if CP includes limit 10 both nodes would result in 20 rows.
 *                                    The result is incomplete
 *                                    ResultDescription needs to contain limit 10
 * </pre>
 */
public interface Consumer {

    Plan consume(QueriedRelation relation, ConsumerContext context);
}

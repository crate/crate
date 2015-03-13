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

package io.crate.operation;

import io.crate.breaker.RamAccountingContext;
import io.crate.operation.projectors.ResultProvider;
import io.crate.planner.node.dql.MergeNode;

public interface DownstreamOperation {

    /**
     * all that is needed for executing a downstream operation is a pagedownstream
     * that receives pages and hands them over to a projector chain which
     * puts its results into the given <code>resultProvider</code>
     * @param mergeNode the execution node that contains the operations parameters
     * @param resultProvider the result provider where results will end up
     * @param ramAccountingContext for accounting bits and bytes used in this operation
     * @return a PageDownstream who consumes pages from upstreams, the entry-point for this operation.
     */
    public PageDownstream getAndInitPageDownstream(MergeNode mergeNode,
                                                   ResultProvider resultProvider,
                                                   RamAccountingContext ramAccountingContext);
}

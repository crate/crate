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

package io.crate.operation.join.nestedloop;

import com.google.common.base.Optional;
import io.crate.executor.FetchedRowsPageableTaskResult;
import io.crate.executor.PageInfo;
import io.crate.executor.PageableTaskResult;
import io.crate.executor.TaskResult;

import java.io.IOException;

/**
 * paging that loads both relations to ram and pages through the
 * fetched projection results.
 */
class FetchedPagingNestedLoopStrategy extends OneShotNestedLoopStrategy {

    public FetchedPagingNestedLoopStrategy(NestedLoopOperation nestedLoopOperation,
                                           NestedLoopExecutorService nestedLoopExecutorService) {
        super(nestedLoopOperation, nestedLoopExecutorService);
    }

    @Override
    public TaskResult emptyResult() {
        return PageableTaskResult.EMPTY_PAGEABLE_RESULT;
    }

    @Override
    public void onFirstJoin(JoinContext joinContext) {
        // we can close the context as we produced ALL results in one batch
        try {
            joinContext.close();
        } catch (IOException e) {
            nestedLoopOperation.logger.error("error closing joinContext after {} NestedLoop execution", e, name());
        }
    }

    @Override
    public TaskResult produceFirstResult(Object[][] rows, Optional<PageInfo> pageInfo, JoinContext joinContext) {
        assert pageInfo.isPresent() : "pageInfo is not present for " + name();
        return FetchedRowsPageableTaskResult.forArray(rows, 0, pageInfo.get());
    }

    @Override
    public String name() {
        return "fetched paging";
    }
}

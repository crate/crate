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
import com.google.common.util.concurrent.FutureCallback;
import io.crate.executor.PageInfo;
import io.crate.executor.TaskResult;
import io.crate.operation.projectors.Projector;

/**
 * a strategy handling the moving/distinct parts for the different ways
 * to execute a JOIN using the NestedLoopOperation
 */
interface NestedLoopStrategy {

    public static interface NestedLoopExecutor {
        public void joinInnerPage();
        public void onNewOuterPage(TaskResult taskResult);
    }

    int rowsToProduce(Optional<PageInfo> pageInfo);

    void onFirstJoin(JoinContext joinContext);

    TaskResult produceFirstResult(Object[][] rows, Optional<PageInfo> pageInfo, JoinContext joinContext);

    String name();

    NestedLoopExecutor executor(JoinContext ctx, Optional<PageInfo> pageInfo, Projector downstream, FutureCallback<Void> callback);
}

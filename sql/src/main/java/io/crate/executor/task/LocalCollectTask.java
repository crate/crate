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

package io.crate.executor.task;

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.Task;
import io.crate.operator.operations.collect.CollectOperation;
import io.crate.planner.node.dql.CollectNode;

import java.util.ArrayList;
import java.util.List;


/**
 * A collect task which returns one future and runs a  collectOperation locally and synchronous
 */
public class LocalCollectTask implements Task<Object[][]> {

    private final CollectNode collectNode;
    private final CollectOperation collectOperation;
    private final List<ListenableFuture<Object[][]>> resultList;

    public LocalCollectTask(CollectOperation collectOperation, CollectNode collectNode) {
        this.collectNode = collectNode;
        this.collectOperation = collectOperation;
        this.resultList = new ArrayList<>(1);
    }

    @Override
    public void start() {
        resultList.add(collectOperation.collect(collectNode));
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return resultList;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        // ignored, comes always first
    }
}

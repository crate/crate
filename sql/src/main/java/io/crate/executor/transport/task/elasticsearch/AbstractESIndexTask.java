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

package io.crate.executor.transport.task.elasticsearch;

import io.crate.Constants;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.task.AbstractChainedTask;
import io.crate.planner.node.dml.ESIndexNode;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;

public abstract class AbstractESIndexTask<T extends TaskResult> extends AbstractChainedTask<T> {

    protected final ESIndexNode node;

    public AbstractESIndexTask(ESIndexNode node) {
        super();
        this.node = node;
    }

    protected IndexRequest buildIndexRequest(String index,
                                             BytesReference source,
                                             String id,
                                             @Nullable String routingValue) {
        IndexRequest request = new IndexRequest(index, Constants.DEFAULT_MAPPING_TYPE);
        request.create(true);
        request.source(source, false);
        request.id(id);
        request.routing(routingValue);
        return request;
    }
}

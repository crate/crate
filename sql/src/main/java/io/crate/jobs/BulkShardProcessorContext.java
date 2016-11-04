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

package io.crate.jobs;

import io.crate.executor.transport.ShardRequest;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class BulkShardProcessorContext extends AbstractExecutionSubContext {

    private static final Logger LOGGER = Loggers.getLogger(BulkShardProcessorContext.class);

    private final BulkShardProcessor<? extends ShardRequest> bulkShardProcessor;

    public BulkShardProcessorContext(int id, BulkShardProcessor<? extends ShardRequest> bulkShardProcessor) {
        super(id, LOGGER);
        this.bulkShardProcessor = bulkShardProcessor;
    }

    @Override
    protected void innerStart() {
        bulkShardProcessor.close();
    }

    @Override
    protected void innerKill(@Nonnull Throwable t) {
        bulkShardProcessor.kill(t);
    }

    @Override
    protected void innerClose(@Nullable Throwable t) {
        if (t != null) {
            bulkShardProcessor.kill(t);
        }
    }

    public boolean add(String indexName,
                       ShardRequest.Item item,
                       @Nullable String routing) {
        return bulkShardProcessor.add(indexName, item, routing);
    }

    @Override
    public String name() {
        return "bulk-update-by-id";
    }
}

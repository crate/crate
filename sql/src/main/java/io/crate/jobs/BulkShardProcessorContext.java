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

import io.crate.analyze.symbol.Symbol;
import org.elasticsearch.action.bulk.BulkShardProcessor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class BulkShardProcessorContext extends AbstractExecutionSubContext {

    private final BulkShardProcessor bulkShardProcessor;

    public BulkShardProcessorContext(int id, BulkShardProcessor bulkShardProcessor) {
        super(id);
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
                       String id,
                       @Nullable Symbol[] assignments,
                       @Nullable Object[] missingAssignments,
                       @Nullable String routing,
                       @Nullable Long version) {
        return bulkShardProcessor.add(indexName, id, assignments, missingAssignments, routing, version);
    }

    @Override
    public String name() {
        return "bulk-update-by-id";
    }
}

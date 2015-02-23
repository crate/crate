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

package io.crate.operation.collect;

import com.google.common.base.Optional;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Shard based context used during collect operations.
 */
public class ShardCollectContext implements Releasable {

    private Optional<SearchContext> context;

    public ShardCollectContext() {
        context = Optional.absent();
    }

    public Optional<SearchContext> context() {
        return context;
    }

    public void context(SearchContext context) {
        this.context = Optional.of(context);
    }

    @Override
    public void close() throws ElasticsearchException {
        if (this.context.isPresent()) {
            Releasables.close(this.context.get());
        }
    }
}

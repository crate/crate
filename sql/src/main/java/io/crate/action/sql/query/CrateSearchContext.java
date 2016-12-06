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

package io.crate.action.sql.query;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexShard;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicBoolean;

public class CrateSearchContext {

    private final long id;
    private final Engine.Searcher engineSearcher;
    private final IndexService indexService;
    private final IndexShard indexShard;
    private final Query query;
    @Nullable
    private final Float minScore;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public CrateSearchContext(long id,
                              Engine.Searcher engineSearcher,
                              IndexService indexService,
                              IndexShard indexShard,
                              Query query,
                              @Nullable Float minScore) {
        this.id = id;
        this.engineSearcher = engineSearcher;
        this.indexService = indexService;
        this.indexShard = indexShard;
        this.query = query;
        this.minScore = minScore;
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            engineSearcher.close();
        }
    }

    public IndexShard indexShard() {
        return indexShard;
    }

    public IndexSearcher searcher() {
        return engineSearcher.searcher();
    }

    public Query query() {
        return query;
    }

    public Float minScore() {
        return minScore;
    }

    public MapperService mapperService() {
        return indexService.mapperService();
    }

    public IndexFieldDataService fieldData() {
        return indexService.fieldData();
    }

    public long id() {
        return id;
    }
}

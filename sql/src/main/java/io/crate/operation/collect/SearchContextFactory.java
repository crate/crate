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

import io.crate.action.sql.query.CrateSearchContext;
import io.crate.analyze.WhereClause;
import io.crate.lucene.LuceneQueryBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;

@Singleton
public class SearchContextFactory {

    private LuceneQueryBuilder luceneQueryBuilder;

    @Inject
    public SearchContextFactory(LuceneQueryBuilder luceneQueryBuilder) {
        this.luceneQueryBuilder = luceneQueryBuilder;
    }

    public CrateSearchContext createContext(
        int jobSearchContextId,
        IndexShard indexshard,
        Engine.Searcher engineSearcher,
        WhereClause whereClause) {

        IndexService indexService = indexshard.indexService();
        LuceneQueryBuilder.Context context = luceneQueryBuilder.convert(
            whereClause, indexService.mapperService(), indexService.fieldData(), indexService.cache());
        return new CrateSearchContext(
            jobSearchContextId,
            engineSearcher,
            indexService,
            indexshard,
            context.query(),
            context.minScore()
        );
    }
}

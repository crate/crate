/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.collect.collectors;

import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchConsumer;
import io.crate.data.Input;
import io.crate.operation.collect.BatchIteratorCollectorBridge;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.util.Collection;
import java.util.List;

public class CrateDocCollectorBuilder implements CrateCollector.Builder {

    private final IndexSearcher indexSearcher;
    private final Query query;
    private final Float minScore;
    private final boolean doScores;
    private final CollectorContext collectorContext;
    private final RamAccountingContext ramAccountingContext;
    private final List<Input<?>> inputs;
    private final Collection<? extends LuceneCollectorExpression<?>> expressions;

    public CrateDocCollectorBuilder(IndexSearcher indexSearcher,
                                    Query query,
                                    Float minScore,
                                    boolean doScores,
                                    CollectorContext collectorContext,
                                    RamAccountingContext ramAccountingContext,
                                    List<Input<?>> inputs,
                                    Collection<? extends LuceneCollectorExpression<?>> expressions) {
        this.indexSearcher = indexSearcher;
        this.query = query;
        this.minScore = minScore;
        this.doScores = doScores;
        this.collectorContext = collectorContext;
        this.ramAccountingContext = ramAccountingContext;
        this.inputs = inputs;
        this.expressions = expressions;
    }

    @Override
    public CrateCollector build(BatchConsumer consumer) {
        LuceneBatchIterator batchIterator = new LuceneBatchIterator(
            indexSearcher,
            query,
            minScore,
            doScores,
            collectorContext,
            ramAccountingContext,
            inputs,
            expressions
        );
        return BatchIteratorCollectorBridge.newInstance(batchIterator, consumer);
    }
}

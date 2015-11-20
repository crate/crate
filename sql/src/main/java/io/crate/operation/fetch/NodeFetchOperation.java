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

package io.crate.operation.fetch;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import io.crate.Streamer;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbols;
import io.crate.executor.transport.StreamBucket;
import io.crate.metadata.TableIdent;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneReferenceResolver;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.IndexService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class NodeFetchOperation {

    private static class TableFetchInfo {

        private final Streamer<?>[] streamers;
        private final Collection<Reference> refs;
        private final FetchContext fetchContext;

        public TableFetchInfo(Collection<Reference> refs, FetchContext fetchContext) {
            this.refs = refs;
            this.fetchContext = fetchContext;
            this.streamers = Symbols.streamerArray(refs);
        }

        public Streamer<?>[] streamers() {
            return streamers;
        }

        public FetchCollector createCollector(int readerId) {
            IndexService indexService = fetchContext.indexService(readerId);
            LuceneReferenceResolver resolver = new LuceneReferenceResolver(indexService.mapperService());
            ArrayList<LuceneCollectorExpression<?>> exprs = new ArrayList<>(refs.size());
            for (Reference reference : refs) {
                exprs.add(resolver.getImplementation(reference.info()));
            }
            return new FetchCollector(exprs,
                    indexService.mapperService(),
                    fetchContext.searcher(readerId),
                    indexService.fieldData(),
                    readerId
            );
        }
    }

    private HashMap<TableIdent, TableFetchInfo> getTableFetchInfos(FetchContext fetchContext) {
        HashMap<TableIdent, TableFetchInfo> result = new HashMap<>(fetchContext.toFetch().size());
        for (Map.Entry<TableIdent, Collection<Reference>> entry : fetchContext.toFetch().entrySet()) {
            TableFetchInfo tableFetchInfo = new TableFetchInfo(entry.getValue(), fetchContext);
            result.put(entry.getKey(), tableFetchInfo);
        }
        return result;
    }

    public IntObjectMap<StreamBucket> doFetch(
            FetchContext fetchContext, IntObjectMap<IntContainer> toFetch) throws Exception {

        IntObjectOpenHashMap<StreamBucket> fetched = new IntObjectOpenHashMap<>(toFetch.size());
        HashMap<TableIdent, TableFetchInfo> tableFetchInfos = getTableFetchInfos(fetchContext);

        for (IntObjectCursor<IntContainer> toFetchCursor : toFetch) {
            TableIdent ident = fetchContext.tableIdent(toFetchCursor.key);
            TableFetchInfo tfi = tableFetchInfos.get(ident);
            assert tfi != null;
            StreamBucket.Builder builder = new StreamBucket.Builder(tfi.streamers());
            tfi.createCollector(toFetchCursor.key).collect(toFetchCursor.value, builder);
            fetched.put(toFetchCursor.key, builder.build());
        }
        return fetched;

    }
}

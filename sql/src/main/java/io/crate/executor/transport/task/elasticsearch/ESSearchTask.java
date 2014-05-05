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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.exceptions.ExceptionHelper;
import io.crate.executor.Task;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.node.dql.ESSearchNode;
import io.crate.planner.symbol.Reference;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ESSearchTask implements Task<Object[][]> {

    private final ESLogger logger = Loggers.getLogger(this.getClass());

    private final ESSearchNode searchNode;
    private final TransportSearchAction transportSearchAction;
    private final SettableFuture<Object[][]> result;
    private final List<ListenableFuture<Object[][]>> results;
    private final ESQueryBuilder queryBuilder;

    public ESSearchTask(ESSearchNode searchNode,
                        TransportSearchAction transportSearchAction) {
        this.searchNode = searchNode;
        this.transportSearchAction = transportSearchAction;
        this.queryBuilder = new ESQueryBuilder();

        result = SettableFuture.create();
        results = Arrays.<ListenableFuture<Object[][]>>asList(result);
    }

    @Override
    public void start() {
        final SearchRequest request = new SearchRequest();

        final ESFieldExtractor[] extractor = buildExtractor(searchNode.outputs());
        final int numColumns = searchNode.outputs().size();

        try {
            request.source(queryBuilder.convert(searchNode), false);
            request.indices(searchNode.indices());
            request.routing(searchNode.whereClause().clusteredBy().orNull());

            if (logger.isDebugEnabled()) {
                logger.debug(request.source().toUtf8());
            }

            transportSearchAction.execute(request, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    if (searchResponse.getFailedShards() > 0) {
                        try {
                            ExceptionHelper.exceptionOnSearchShardFailures(searchResponse.getShardFailures());
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    } else {
                        final SearchHit[] hits = searchResponse.getHits().getHits();
                        final Object[][] rows = new Object[hits.length][numColumns];

                        for (int r = 0; r < hits.length; r++) {
                            rows[r] = new Object[numColumns];
                            for (int c = 0; c < numColumns; c++) {
                                rows[r][c] = extractor[c].extract(hits[r]);
                            }
                        }

                        result.set(rows);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    result.setException(e);
                }
            });
        } catch (IOException e) {
            result.setException(e);
        }
    }

    private ESFieldExtractor[] buildExtractor(final List<? extends Reference> outputs) {
        ESFieldExtractor[] extractors = new ESFieldExtractor[outputs.size()];
        int i = 0;
        for (final Reference reference : outputs) {
            final ColumnIdent columnIdent = reference.info().ident().columnIdent();
            if (DocSysColumns.VERSION.equals(columnIdent)) {
                extractors[i] = new ESFieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return hit.getVersion();
                    }
                };
            } else if (DocSysColumns.ID.equals(columnIdent)) {
                extractors[i] = new ESFieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return new BytesRef(hit.getId());
                    }
                };
            } else if (DocSysColumns.DOC.equals(columnIdent)) {
                extractors[i] = new ESFieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return hit.getSource();
                    }
                };
            } else if (DocSysColumns.RAW.equals(columnIdent)) {
                extractors[i] = new ESFieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return hit.getSourceRef().toBytesRef();
                    }
                };
            } else if (DocSysColumns.SCORE.equals(columnIdent)) {
                extractors[i] = new ESFieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return hit.getScore();
                    }
                };
            } else if (searchNode.partitionBy().contains(reference.info())) {
                extractors[i] = new ESFieldExtractor.PartitionedByColumnExtractor(
                        reference, searchNode.partitionBy()
                );
            } else {
                extractors[i] = new ESFieldExtractor.Source(columnIdent);
            }
            i++;
        }
        return extractors;
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        throw new UnsupportedOperationException("Can't have upstreamResults");
    }

}

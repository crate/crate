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

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CrateResultSorter {

    private static final boolean optimizeSingleShard = true;

    /**
     * copied from SearchPhaseController, to manually set offset
     *
     * @param resultsArr Shard result holder
     * @param offset the number of results to skipd
     * @param limit the number of results to return at max
     */
    public ScoreDoc[] sortDocs(AtomicArray<? extends QuerySearchResultProvider> resultsArr, int offset, int limit) throws IOException {
        List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> results = resultsArr.asList();
        if (results.isEmpty()) {
            return SearchPhaseController.EMPTY_DOCS;
        }

        if (optimizeSingleShard) {
            boolean canOptimize = false;
            QuerySearchResult result = null;
            int shardIndex = -1;
            if (results.size() == 1) {
                canOptimize = true;
                result = results.get(0).value.queryResult();
                shardIndex = results.get(0).index;
            } else {
                // lets see if we only got hits from a single shard, if so, we can optimize...
                for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : results) {
                    if (entry.value.queryResult().topDocs().scoreDocs.length > 0) {
                        if (result != null) { // we already have one, can't really optimize
                            canOptimize = false;
                            break;
                        }
                        canOptimize = true;
                        result = entry.value.queryResult();
                        shardIndex = entry.index;
                    }
                }
            }
            if (canOptimize) {
                ScoreDoc[] scoreDocs = result.topDocs().scoreDocs;
                if (scoreDocs.length == 0 || scoreDocs.length < offset) {
                    return SearchPhaseController.EMPTY_DOCS;
                }

                int resultDocsSize = result.size();
                if ((scoreDocs.length - offset) < resultDocsSize) {
                    resultDocsSize = scoreDocs.length - offset;
                }
                ScoreDoc[] docs = new ScoreDoc[resultDocsSize];
                for (int i = 0; i < resultDocsSize; i++) {
                    ScoreDoc scoreDoc = scoreDocs[offset + i];
                    scoreDoc.shardIndex = shardIndex;
                    docs[i] = scoreDoc;
                }
                return docs;
            }
        }

        @SuppressWarnings("unchecked")
        AtomicArray.Entry<? extends QuerySearchResultProvider>[] sortedResults = results.toArray(new AtomicArray.Entry[results.size()]);
        Arrays.sort(sortedResults, SearchPhaseController.QUERY_RESULT_ORDERING);
        QuerySearchResultProvider firstResult = sortedResults[0].value;

        final Sort sort;
        if (firstResult.queryResult().topDocs() instanceof TopFieldDocs) {
            TopFieldDocs firstTopDocs = (TopFieldDocs) firstResult.queryResult().topDocs();
            sort = new Sort(firstTopDocs.fields);
        } else {
            sort = null;
        }

        // Need to use the length of the resultsArr array, since the slots will be based on the position in the resultsArr array
        TopDocs[] shardTopDocs = new TopDocs[resultsArr.length()];
        if (firstResult.includeFetch()) {
            // if we did both query and fetch on the same go, we have fetched all the docs from each shards already, use them...
            // this is also important since we shortcut and fetch only docs from "from" and up to "size"
            limit *= sortedResults.length;
        }
        for (AtomicArray.Entry<? extends QuerySearchResultProvider> sortedResult : sortedResults) {
            TopDocs topDocs = sortedResult.value.queryResult().topDocs();
            // the 'index' field is the position in the resultsArr atomic array
            shardTopDocs[sortedResult.index] = topDocs;
        }
        // TopDocs#merge can't deal with null shard TopDocs
        for (int i = 0; i < shardTopDocs.length; i++) {
            if (shardTopDocs[i] == null) {
                shardTopDocs[i] = Lucene.EMPTY_TOP_DOCS;
            }
        }
        TopDocs mergedTopDocs = TopDocs.merge(sort, offset, limit, shardTopDocs);
        return mergedTopDocs.scoreDocs;
    }

    /**
     * Builds an array, with potential null elements, with docs to load.
     */
    public void fillDocIdsToLoad(AtomicArray<IntArrayList> docsIdsToLoad, ScoreDoc[] shardDocs, int offset) {
        assert offset >= 0;
        int skip = offset;
        for (ScoreDoc shardDoc : shardDocs) {

            IntArrayList list = docsIdsToLoad.get(shardDoc.shardIndex);
            if (list == null) {
                list = new IntArrayList(); // can't be shared!, uses unsafe on it later on
                docsIdsToLoad.set(shardDoc.shardIndex, list);
            }

            // only add docIds if offset is already skipped
            if (skip <= 0) {
                list.add(shardDoc.doc);
            }

            skip--;
        }
    }
}

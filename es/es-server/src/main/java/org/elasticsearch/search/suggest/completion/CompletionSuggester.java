/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.TopSuggestDocs;
import org.apache.lucene.search.suggest.document.TopSuggestDocsCollector;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggester;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CompletionSuggester extends Suggester<CompletionSuggestionContext> {

    public static final CompletionSuggester INSTANCE = new CompletionSuggester();

    private CompletionSuggester() {}

    @Override
    protected Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(String name,
            final CompletionSuggestionContext suggestionContext, final IndexSearcher searcher, CharsRefBuilder spare) throws IOException {
        if (suggestionContext.getFieldType() != null) {
            final CompletionFieldMapper.CompletionFieldType fieldType = suggestionContext.getFieldType();
            CompletionSuggestion completionSuggestion =
                new CompletionSuggestion(name, suggestionContext.getSize(), suggestionContext.isSkipDuplicates());
            spare.copyUTF8Bytes(suggestionContext.getText());
            CompletionSuggestion.Entry completionSuggestEntry = new CompletionSuggestion.Entry(
                new Text(spare.toString()), 0, spare.length());
            completionSuggestion.addTerm(completionSuggestEntry);
            int shardSize = suggestionContext.getShardSize() != null ? suggestionContext.getShardSize() : suggestionContext.getSize();
            TopSuggestGroupDocsCollector collector = new TopSuggestGroupDocsCollector(shardSize, suggestionContext.isSkipDuplicates());
            suggest(searcher, suggestionContext.toQuery(), collector);
            int numResult = 0;
            for (TopSuggestDocs.SuggestScoreDoc suggestDoc : collector.get().scoreLookupDocs()) {
                // collect contexts
                Map<String, Set<CharSequence>> contexts = Collections.emptyMap();
                if (fieldType.hasContextMappings()) {
                    List<CharSequence> rawContexts = collector.getContexts(suggestDoc.doc);
                    if (rawContexts.size() > 0) {
                        contexts = fieldType.getContextMappings().getNamedContexts(rawContexts);
                    }
                }
                if (numResult++ < suggestionContext.getSize()) {
                    CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(suggestDoc.doc,
                        new Text(suggestDoc.key.toString()), suggestDoc.score, contexts);
                    completionSuggestEntry.addOption(option);
                } else {
                    break;
                }
            }
            return completionSuggestion;
        }
        return null;
    }

    private static void suggest(IndexSearcher searcher, CompletionQuery query, TopSuggestDocsCollector collector) throws IOException {
        query = (CompletionQuery) query.rewrite(searcher.getIndexReader());
        Weight weight = query.createWeight(searcher, collector.needsScores(), 1f);
        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            BulkScorer scorer = weight.bulkScorer(context);
            if (scorer != null) {
                try {
                    scorer.score(collector.getLeafCollector(context), context.reader().getLiveDocs());
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }
        }
    }
}

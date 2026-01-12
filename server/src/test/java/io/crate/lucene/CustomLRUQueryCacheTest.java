/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.crate.lucene;

import static io.crate.testing.Asserts.assertThat;
import static org.apache.lucene.tests.util.LuceneTestCase.newSearcher;
import static org.apache.lucene.util.RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Test;

public class CustomLRUQueryCacheTest extends CrateLuceneTestCase {

    private static final QueryCachingPolicy ALWAYS_CACHE =
        new QueryCachingPolicy() {

            @Override
            public void onUse(Query query) {}

            @Override
            public boolean shouldCache(Query query) throws IOException {
                return true;
            }
        };

    @Test
    public void testQuerySizeBytesAreCached() throws IOException {
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new StringField("foo", "bar", Field.Store.YES));
            doc.add(new StringField("foo", "quux", Field.Store.YES));
            w.addDocument(doc);
            w.commit();
            final IndexReader reader = w.getReader();
            final IndexSearcher searcher = newSearcher(reader);
            final CustomLRUQueryCache queryCache =
                new CustomLRUQueryCache(1000000, 10000000, _ -> true, Float.POSITIVE_INFINITY);
            searcher.setQueryCache(queryCache);
            searcher.setQueryCachingPolicy(ALWAYS_CACHE);
            StringBuilder sb = new StringBuilder();
            sb.append("a".repeat(100));
            String term = sb.toString();
            TermQuery termQuery = new TermQuery(new Term("foo", term));
            long expectedQueryInBytes =
                LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + RamUsageEstimator.sizeOf(termQuery, 32);
            searcher.search(new ConstantScoreQuery(termQuery), 1);

            assertThat(queryCache.getUniqueQueries().size()).isEqualTo(1);
            assertThat(queryCache.getUniqueQueries().get(termQuery).queryRamBytesUsed()).isEqualTo(expectedQueryInBytes);
            reader.close();
        }
    }

    public void testCacheRamBytesWithALargeTermQuery() throws IOException {
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new StringField("foo", "bar", Field.Store.YES));
            doc.add(new StringField("foo", "quux", Field.Store.YES));
            w.addDocument(doc);
            w.commit();
            final IndexReader reader = w.getReader();
            final IndexSearcher searcher = newSearcher(reader);
            final CustomLRUQueryCache queryCache =
                new CustomLRUQueryCache(1000000, 10000000, _ -> true, Float.POSITIVE_INFINITY);
            searcher.setQueryCache(queryCache);
            searcher.setQueryCachingPolicy(ALWAYS_CACHE);
            StringBuilder sb = new StringBuilder();
            // Create a large string for the field value so it certainly exceeds the default query size we
            // use ie 1024 bytes.
            sb.append("a".repeat(1200));
            String longTerm = sb.toString();
            TermQuery must = new TermQuery(new Term("foo", longTerm));
            long queryInBytes = RamUsageEstimator.sizeOf(must, 32);
            assertThat(queryInBytes).isGreaterThan(QUERY_DEFAULT_RAM_BYTES_USED);
            searcher.search(new ConstantScoreQuery(must), 1);

            assertThat(queryCache.cachedQueries().size()).isEqualTo(1);
            assertThat(queryCache.ramBytesUsed()).isGreaterThanOrEqualTo(queryInBytes);
            reader.close();
        }
    }

}

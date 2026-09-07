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
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.QueryTester;

public class CustomLRUQueryCacheTest extends CrateDummyClusterServiceUnitTest {

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

    @Test
    public void test_standalone_generic_function_query_is_cached_as_small_cacheable_query() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (a int, b int)"
        );
        builder.indexValues(List.of("a", "b"), 1, 1);
        try (QueryTester tester = builder.build()) {
            Query query = tester.toQuery("abs(a) * abs(b) = 1");
            assertThat(query).isInstanceOf(GenericFunctionQuery.class);

            IndexSearcher searcher = tester.searcher();
            CustomLRUQueryCache queryCache =
                new CustomLRUQueryCache(1000000, 10000000, _ -> true, Float.POSITIVE_INFINITY);
            searcher.setQueryCache(queryCache);
            searcher.setQueryCachingPolicy(ALWAYS_CACHE);
            searcher.count(query);

            assertThat(queryCache.getUniqueQueries()).hasSize(1);
            Query cachedQuery = queryCache.getUniqueQueries().keySet().iterator().next();
            assertThat(cachedQuery).isInstanceOf(GenericFunctionQuery.SmallCacheableQuery.class);
        }
    }

    @Test
    public void test_generic_function_query_in_boolean_query_is_cached_as_small_cacheable_query() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (a int, b int)"
        );
        builder.indexValues(List.of("a", "b"), 1, 1);
        try (QueryTester tester = builder.build()) {
            Query query = tester.toQuery("abs(a) = 1 AND abs(b) = 1");
            assertThat(query).isInstanceOf(BooleanQuery.class);
            for (BooleanClause clause : ((BooleanQuery) query).clauses()) {
                assertThat(clause.query()).isInstanceOf(GenericFunctionQuery.class);
            }

            IndexSearcher searcher = tester.searcher();
            CustomLRUQueryCache queryCache =
                new CustomLRUQueryCache(1000000, 10000000, _ -> true, Float.POSITIVE_INFINITY);
            searcher.setQueryCache(queryCache);
            searcher.setQueryCachingPolicy(ALWAYS_CACHE);
            searcher.count(query);

            // Cache has 3 entries: 1 composite BooleanQuery from IndexSearcher.createWeight
            // and 2 leaf queries created in BooleanWeight ctor.
            for (Query cachedQuery : queryCache.getUniqueQueries().keySet()) {
                if (cachedQuery instanceof BooleanQuery booleanQuery) {
                    for (BooleanClause clause : booleanQuery.clauses()) {
                        assertThat(clause.query()).isInstanceOf(GenericFunctionQuery.SmallCacheableQuery.class);
                    }
                } else {
                    assertThat(cachedQuery).isInstanceOf(GenericFunctionQuery.SmallCacheableQuery.class);
                }
            }
        }
    }

}

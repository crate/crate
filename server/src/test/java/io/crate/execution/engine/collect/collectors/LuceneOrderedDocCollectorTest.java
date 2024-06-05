/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.collect.collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.StreamSupport;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.shard.ShardId;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import io.crate.analyze.OrderBy;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dml.StringIndexer;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.NullSentinelValues;
import io.crate.expression.reference.doc.lucene.ScoreCollectorExpression;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.DataTypes;

public class LuceneOrderedDocCollectorTest extends RandomizedTest {

    private static final SimpleReference REFERENCE = new SimpleReference(
        new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, "table"), "value"),
        RowGranularity.DOC,
        DataTypes.LONG,
        0,
        null
    );

    private Directory createLuceneIndex() throws IOException {
        Path tmpDir = newTempDir();
        Directory index = FSDirectory.open(tmpDir);
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig cfg = new IndexWriterConfig(analyzer);
        IndexWriter w = new IndexWriter(index, cfg);
        for (long i = 0L; i < 4; i++) {
            if (i < 2) {
                addDocToLucene(w, i + 1);
            } else {
                addDocToLucene(w, null);
            }
            w.commit();
        }
        w.close();
        return index;
    }

    private void addDocToLucene(IndexWriter w, Long value) throws IOException {
        Document doc = new Document();
        if (value != null) {
            doc.add(new LongField("value", value, Field.Store.NO));
        } else {
            // Create a placeholder field
            doc.add(new SortedDocValuesField("null_value", new BytesRef("null")));
        }
        w.addDocument(doc);
    }

    private TopFieldDocs search(IndexReader reader, Query searchAfterQuery, Sort sort) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query;
        if (searchAfterQuery != null) {
            // searchAfterQuery is actually a query for all before last doc, so negate it
            query = Queries.not(searchAfterQuery);
        } else {
            query = new MatchAllDocsQuery();
        }
        TopFieldDocs docs = searcher.search(query, 10, sort);
        return docs;
    }

    private Long[] nextPageQuery(IndexReader reader, FieldDoc lastCollected, boolean reverseFlag, boolean nullFirst) throws IOException {
        OrderBy orderBy = new OrderBy(
            List.of(REFERENCE),
            new boolean[]{reverseFlag},
            new boolean[]{nullFirst});

        SortField sortField = new SortedNumericSortField("value", SortField.Type.LONG, reverseFlag);
        Long missingValue = (Long) NullSentinelValues.nullSentinelForScoreDoc(orderBy, 0);
        sortField.setMissingValue(missingValue);
        Sort sort = new Sort(sortField);

        OptimizeQueryForSearchAfter queryForSearchAfter = new OptimizeQueryForSearchAfter(orderBy);
        Query nextPageQuery = queryForSearchAfter.apply(lastCollected);
        TopFieldDocs result = search(reader, nextPageQuery, sort);
        Long[] results = new Long[result.scoreDocs.length];
        for (int i = 0; i < result.scoreDocs.length; i++) {
            Long value = (Long) ((FieldDoc) result.scoreDocs[i]).fields[0];
            results[i] = value.equals(missingValue) ? null : value;
        }
        return results;
    }


    @Test
    public void testNextPageQueryWithLastCollectedNullValue() {
        FieldDoc fieldDoc = new FieldDoc(1, 0, new Object[]{null});
        OrderBy orderBy = new OrderBy(Collections.singletonList(REFERENCE), new boolean[]{false}, new boolean[]{false});

        OptimizeQueryForSearchAfter queryForSearchAfter = new OptimizeQueryForSearchAfter(orderBy);

        queryForSearchAfter.apply(fieldDoc);
    }

    // search after queries
    @Test
    public void testSearchAfterQueriesNullsLast() throws Exception {
        Directory index = createLuceneIndex();
        IndexReader reader = DirectoryReader.open(index);

        // reverseOrdering = false, nulls First = false
        // 1  2  null null
        //    ^  (lastCollected = 2)

        FieldDoc afterDoc = new FieldDoc(0, 0, new Object[]{2L});
        Long[] result = nextPageQuery(reader, afterDoc, false, false);
        assertThat(result).isEqualTo(new Long[]{2L, null, null});

        // reverseOrdering = false, nulls First = false
        // 1  2  null null
        //       ^
        afterDoc = new FieldDoc(0, 0, new Object[]{NullSentinelValues.nullSentinelForScoreDoc(DataTypes.LONG, false, null)});
        result = nextPageQuery(reader, afterDoc, false, false);
        assertThat(result).isEqualTo(new Long[]{null, null});

        // reverseOrdering = true, nulls First = false
        // 2  1  null null
        //    ^
        afterDoc = new FieldDoc(0, 0, new Object[]{1L});
        result = nextPageQuery(reader, afterDoc, true, false);
        assertThat(result).isEqualTo(new Long[]{1L, null, null});

        // reverseOrdering = true, nulls First = false
        // 2  1  null null
        //       ^
        afterDoc = new FieldDoc(0, 0, new Object[]{NullSentinelValues.nullSentinelForScoreDoc(DataTypes.LONG, true, false)});
        result = nextPageQuery(reader, afterDoc, true, false);
        assertThat(result).isEqualTo(new Long[]{null, null});

        reader.close();
    }

    @Test
    public void testSearchAfterQueriesNullsFirst() throws Exception {
        Directory index = createLuceneIndex();
        IndexReader reader = DirectoryReader.open(index);

        // reverseOrdering = false, nulls First = true
        // null, null, 1, 2
        //                ^  (lastCollected = 2L)

        FieldDoc afterDoc = new FieldDoc(0, 0, new Object[]{2L});
        Long[] result = nextPageQuery(reader, afterDoc, false, true);
        assertThat(result).isEqualTo(new Long[]{2L});

        // reverseOrdering = false, nulls First = true
        // null, null, 1, 2
        //       ^
        afterDoc = new FieldDoc(0, 0, new Object[]{NullSentinelValues.nullSentinelForScoreDoc(DataTypes.LONG, false, true)});
        result = nextPageQuery(reader, afterDoc, false, true);
        assertThat(result).isEqualTo(new Long[]{null, null, 1L, 2L});

        // reverseOrdering = true, nulls First = true
        // null, null, 2, 1
        //                ^
        afterDoc = new FieldDoc(0, 0, new Object[]{1L});
        result = nextPageQuery(reader, afterDoc, true, true);
        assertThat(result).isEqualTo(new Long[]{1L});

        // reverseOrdering = true, nulls First = true
        // null, null, 2, 1
        //       ^
        afterDoc = new FieldDoc(0, 0, new Object[]{NullSentinelValues.nullSentinelForScoreDoc(DataTypes.LONG, true, true)});
        result = nextPageQuery(reader, afterDoc, true, true);
        assertThat(result).isEqualTo(new Long[]{null, null, 2L, 1L});

        reader.close();
    }


    @Test
    public void testSearchAfterWithSystemColumn() {
        Reference sysColReference =
            new SimpleReference(
                new ReferenceIdent(
                    new RelationName(Schemas.DOC_SCHEMA_NAME, "table"),
                    DocSysColumns.SCORE), RowGranularity.DOC, DataTypes.FLOAT, 0, null
            );

        OrderBy orderBy = new OrderBy(List.of(sysColReference, REFERENCE),
            new boolean[]{false, false},
            new boolean[]{false, false});

        FieldDoc lastCollected = new FieldDoc(0, 0, new Object[]{2L});

        OptimizeQueryForSearchAfter queryForSearchAfter = new OptimizeQueryForSearchAfter(orderBy);
        Query nextPageQuery = queryForSearchAfter.apply(lastCollected);

        // returns null which leads to reuse of old query without paging optimization
        assertNull(nextPageQuery);
    }

    @Test
    public void testSearchMoreAppliesMinScoreFilter() throws Exception {
        IndexWriter w = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        var fieldType = StringIndexer.FIELD_TYPE;

        for (int i = 0; i < 3; i++) {
            addDoc(w, "x", fieldType, "Arthur");
        }
        addDoc(w, "x", fieldType, "Arthurr"); // not "Arthur" to lower score
        w.commit();
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(w, true, true));

        List<LuceneCollectorExpression<?>> columnReferences = Collections.singletonList(new ScoreCollectorExpression());
        Query query = new FuzzyQuery(new Term("x", "Arthur"), Fuzziness.AUTO.asDistance("Arthur"), 2, 3, true);
        LuceneOrderedDocCollector collector;

        // without minScore filter we get 2 and 2 docs - this is not necessary for the test but is here
        // to make sure the "FuzzyQuery" matches the right documents
        collector = collector(searcher, columnReferences, query, null, true);
        assertThat(StreamSupport.stream(collector.collect().spliterator(), false).count()).isEqualTo(2L);
        assertThat(StreamSupport.stream(collector.collect().spliterator(), false).count()).isEqualTo(2L);

        collector = collector(searcher, columnReferences, query, 0.15f, true);
        int count = 0;
        // initialSearch -> 2 rows
        for (Row row : collector.collect()) {
            assertThat((float) row.get(0), Matchers.greaterThanOrEqualTo(0.15f));
            count++;
        }
        assertThat(count).isEqualTo(2);

        count = 0;
        // searchMore -> 1 row is below minScore
        for (Row row : collector.collect()) {
            assertThat((float) row.get(0), Matchers.greaterThanOrEqualTo(0.15f));
            count++;
        }
        assertThat(count).isEqualTo(1);
    }

    @Test
    public void testSearchNoScores() throws Exception {
        IndexWriter w = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));
        String name = "x";
        var fieldType = StringIndexer.FIELD_TYPE;

        for (int i = 0; i < 3; i++) {
            addDoc(w, name, fieldType, "Arthur");
        }
        addDoc(w, name, fieldType, "Arthur"); // not "Arthur" to lower score
        w.commit();
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(w, true, true));

        List<LuceneCollectorExpression<?>> columnReferences = Collections.singletonList(new ScoreCollectorExpression());
        Query query = new TermQuery(new Term(name, new BytesRef("Arthur")));
        LuceneOrderedDocCollector collector = collector(searcher, columnReferences, query, null, false);
        KeyIterable<ShardId, Row> result = collector.collect();

        assertThat(StreamSupport.stream(result.spliterator(), false).count()).isEqualTo(2L);

        Iterator<Row> values = result.iterator();

        assertThat(values.next().get(0)).isEqualTo(Float.NaN);
        assertThat(values.next().get(0)).isEqualTo(Float.NaN);
    }

    @Test
    public void testSearchWithScores() throws Exception {
        IndexWriter w = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(new KeywordAnalyzer()));

        FieldType fieldType = StringIndexer.FIELD_TYPE;
        for (int i = 0; i < 3; i++) {
            addDoc(w, "x", fieldType, "Arthur");
        }
        addDoc(w, "x", fieldType, "Arthur"); // not "Arthur" to lower score
        w.commit();
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(w, true, true));

        List<LuceneCollectorExpression<?>> columnReferences = Collections.singletonList(new ScoreCollectorExpression());
        Query query = new ConstantScoreQuery(new TermQuery(new Term("x", new BytesRef("Arthur"))));
        LuceneOrderedDocCollector collector = collector(searcher, columnReferences, query, null, true);
        KeyIterable<ShardId, Row> result = collector.collect();

        assertThat(StreamSupport.stream(result.spliterator(), false).count()).isEqualTo(2L);

        Iterator<Row> values = result.iterator();

        assertThat(values.next().get(0)).isEqualTo(1.0F);
        assertThat(values.next().get(0)).isEqualTo(1.0F);
    }

    private static void addDoc(IndexWriter w, String name, FieldType fieldType, String value) throws IOException {
        Document doc = new Document();
        Field field = new Field(name, value, fieldType);
        doc.add(field);
        w.addDocument(doc);
    }

    private LuceneOrderedDocCollector collector(IndexSearcher searcher,
                                                List<LuceneCollectorExpression<?>> columnReferences,
                                                Query query,
                                                @Nullable Float minScore, boolean doDocScores) {
        return new LuceneOrderedDocCollector(
            new ShardId("dummy", UUIDs.base64UUID(), 0),
            searcher,
            query,
            minScore,
            doDocScores,
            2,
            RamAccounting.NO_ACCOUNTING,
            new CollectorContext(Set.of(), UnaryOperator.identity()),
            f -> null,
            new Sort(SortField.FIELD_SCORE),
            columnReferences,
            columnReferences
        );
    }
}

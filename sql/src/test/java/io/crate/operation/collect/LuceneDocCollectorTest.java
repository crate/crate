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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.crate.action.sql.SQLBulkRequest;
import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Bucket;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.operation.Paging;
import io.crate.operation.collect.collectors.OrderedDocCollector;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.doc.lucene.LuceneMissingValue;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.LuceneDocCollectorProvider;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CancellationException;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes = 1)
public class LuceneDocCollectorTest extends SQLTransportIntegrationTest {

    private final static Integer NODE_PAGE_SIZE_HINT = 20;
    private final static String INDEX_NAME = "countries";
    // use higher value here to be sure multiple segment reader exists during collect (not only 1)
    private final static Integer NUMBER_OF_DOCS = 10_000;

    private CollectingRowReceiver rowReceiver = new CollectingRowReceiver();

    private LuceneDocCollectorProvider collectorProvider;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private int originalPageSize;


    @Before
    public void prepare() throws Exception{
        originalPageSize = Paging.PAGE_SIZE;
        Paging.PAGE_SIZE = NODE_PAGE_SIZE_HINT;
        execute("create table \"" + INDEX_NAME + "\" (" +
                " continent string, " +
                " \"countryName\" string," +
                " population integer primary key" +
                ") clustered into 1 shards with (number_of_replicas=0)");
        refresh();
        generateData();
        collectorProvider = new LuceneDocCollectorProvider(internalCluster());
    }

    @After
    public void closeContext() throws Exception {
        Paging.PAGE_SIZE = originalPageSize;
        collectorProvider.close();
    }

    public void generateData() throws Exception {
        Object[][] args = new Object[NUMBER_OF_DOCS][];
        args[0] = new Object[] {"Europe", "Germany", 0};
        args[1] = new Object[] {"Europe", "Austria", 1};
        for (int i = 2; i <= 4; i++) {
            args[i] = new Object[] {"Europe", null, i};
        }
        for (int i = 5; i < NUMBER_OF_DOCS; i++) {
            args[i] = new Object[] {"America", "USA", i};
        }
        sqlExecutor.execBulk("insert into countries (continent, \"countryName\", population) values (?, ?, ?)", args,
                TimeValue.timeValueSeconds(30));
        refresh();
    }

    private CrateCollector createDocCollector(String statement, RowReceiver rowReceiver, Object ... args) {
        return Iterables.getOnlyElement(collectorProvider.createCollectors(statement, rowReceiver, NODE_PAGE_SIZE_HINT, args));
    }


    @Test
    public void testLimitWithoutOrder() throws Exception{
        CrateCollector docCollector = createDocCollector("select \"countryName\" from countries limit 15", rowReceiver);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(15));
    }

    @Test
    public void testOrderedWithLimit() throws Exception{
        CrateCollector docCollector = createDocCollector(
                "select \"countryName\" from countries order by \"countryName\" asc nulls last limit 15", rowReceiver);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(15));
        assertThat(((BytesRef) rowReceiver.rows.get(0)[0]).utf8ToString(), is("Austria"));
        assertThat(((BytesRef) rowReceiver.rows.get(1)[0]).utf8ToString(), is("Germany"));
        assertThat(((BytesRef) rowReceiver.rows.get(2)[0]).utf8ToString(), is("USA"));
        assertThat(((BytesRef) rowReceiver.rows.get(3)[0]).utf8ToString(), is("USA"));
    }

    @Test
    public void testOrderedPauseResume() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(3);
        CrateCollector collector = createDocCollector(
                "select population from countries order by population limit 15", rowReceiver);
        collector.doCollect();

        assertThat(rowReceiver.rows.size(), is(3));
        rowReceiver.resumeUpstream(false); // continue
        assertThat(rowReceiver.rows.size(), is(15));
        for (int i = 0; i < rowReceiver.rows.size();  i++) {
            assertThat((Integer)rowReceiver.rows.get(i)[0], is(i));
        }
        rowReceiver.result(); // shouldn't timeout
    }

    @Test
    public void testPauseBeforeNextTopNSearch() throws Exception {
        Paging.PAGE_SIZE = 5;
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(5);

        CrateCollector collector = createDocCollector(
                "select population from countries order by population limit 20", rowReceiver);
        collector.doCollect();
        rowReceiver.resumeUpstream(false);

        Bucket bucket = rowReceiver.result();
        assertThat(bucket.size(), is(20));

        assertThat(TestingHelpers.printedTable(bucket),
                is("0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnorderedPauseResume() throws Exception {
        int pauseAfter = NUMBER_OF_DOCS - 5;
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(pauseAfter);
        CrateCollector docCollector = createDocCollector("select population from countries", rowReceiver);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(pauseAfter));
        rowReceiver.resumeUpstream(false);

        Bucket bucket = rowReceiver.result();
        assertThat(bucket.size(), is(NUMBER_OF_DOCS));
        assertThat(new ArrayList<>(rowReceiver.rows), containsInAnyOrder(new ArrayList() {{
            for (int i = 0; i < NUMBER_OF_DOCS; i++) {
                add(equalTo(new Object[]{i}));
            }
        }}));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnorderedPauseAfterLastDoc() throws Exception {
        int pauseAfter = NUMBER_OF_DOCS;
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(pauseAfter);
        CrateCollector docCollector = createDocCollector("select population from countries", rowReceiver);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(pauseAfter));
        assertThat(rowReceiver.isFinished(), is(false));
        rowReceiver.resumeUpstream(false);

        Bucket bucket = rowReceiver.result();
        assertThat(bucket.size(), is(NUMBER_OF_DOCS));
        assertThat(new ArrayList<>(rowReceiver.rows), containsInAnyOrder(new ArrayList() {{
            for (int i = 0; i < NUMBER_OF_DOCS; i++) {
                add(equalTo(new Object[]{i}));
            }
        }}));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAsynchronousResume() throws Exception {
        int pauseAfter = NUMBER_OF_DOCS - 100;
        CollectingRowReceiver projector = CollectingRowReceiver.withPauseAfter(pauseAfter);

        CrateCollector docCollector = createDocCollector("select population from countries", projector);
        docCollector.doCollect();
        assertThat(projector.rows.size(), is(pauseAfter));
        projector.resumeUpstream(true);

        projector.result();
        assertThat(projector.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(new ArrayList<>(projector.rows), containsInAnyOrder(new ArrayList() {{
            for (int i = 0; i < NUMBER_OF_DOCS; i++) {
                add(equalTo(new Object[]{i}));
            }
        }}));
    }

    @Test
    public void testKillWhilePaused() throws Exception {
        CollectingRowReceiver projector = CollectingRowReceiver.withPauseAfter(5);
        CrateCollector docCollector = createDocCollector("select \"countryName\" from countries order by 1 limit 15", projector);
        docCollector.doCollect();
        assertThat(projector.rows.size(), is(5));

        docCollector.kill(null);

        expectedException.expect(CancellationException.class);
        projector.result();
    }

    @Test
    public void testOrderedWithLimitHigherThanPageSize() throws Exception{
        CrateCollector docCollector = createDocCollector("select \"countryName\" from countries order by 1 limit ?", rowReceiver, NODE_PAGE_SIZE_HINT + 5);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NODE_PAGE_SIZE_HINT + 5));
        assertThat(((BytesRef) rowReceiver.rows.get(0)[0]).utf8ToString(), is("Austria"));
        assertThat(((BytesRef) rowReceiver.rows.get(1)[0]).utf8ToString(), is("Germany"));
        assertThat(((BytesRef) rowReceiver.rows.get(2)[0]).utf8ToString(), is("USA"));
        assertThat(((BytesRef) rowReceiver.rows.get(3)[0]).utf8ToString(), is("USA"));
    }

    @Test
    public void testOrderedWithNullsGtNodePageSize() throws Exception {
        execute("create table nulls_table (foo integer) clustered into 1 shards with (number_of_replicas=0)");
        Object[][] args = new Object[NODE_PAGE_SIZE_HINT * 2][];
        for (int i = 0; i < NODE_PAGE_SIZE_HINT * 2; i++) {
            args[i] = new Object[]{null};
        }
        sqlExecutor.execBulk("insert into nulls_table (foo) values (?)", args,
                TimeValue.timeValueSeconds(1));
        execute("insert into nulls_table (foo) values (1)");
        refresh();
        CrateCollector docCollector = createDocCollector("select * from nulls_table order by foo desc nulls last limit ?", rowReceiver, NODE_PAGE_SIZE_HINT + 5);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NODE_PAGE_SIZE_HINT + 5));
    }

    @Test
    public void testOrderedWithoutLimit() throws Exception {
        CrateCollector docCollector = createDocCollector("select \"countryName\" from countries order by 1", rowReceiver);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(((BytesRef) rowReceiver.rows.get(0)[0]).utf8ToString(), is("Austria"));
        assertThat(((BytesRef) rowReceiver.rows.get(1)[0]).utf8ToString(), is("Germany"));
        assertThat(((BytesRef) rowReceiver.rows.get(2)[0]).utf8ToString(), is("USA"));
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS -1)[0], is(nullValue()));
    }

    @Test
    public void testOrderedNullsFirstWithoutLimit() throws Exception {
        CrateCollector docCollector = createDocCollector("select \"countryName\" from countries order by 1 nulls first", rowReceiver);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(rowReceiver.rows.get(0)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(1)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(2)[0], is(nullValue()));
        assertThat(((BytesRef) rowReceiver.rows.get(3)[0]).utf8ToString(), is("Austria"));
        assertThat(((BytesRef) rowReceiver.rows.get(4)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef) rowReceiver.rows.get(5)[0]).utf8ToString(), is("USA") );
    }

    @Test
    public void testOrderedDescendingWithoutLimit() throws Exception {
        CrateCollector docCollector = createDocCollector("select \"countryName\" from countries order by 1 desc nulls last", rowReceiver);
        docCollector.doCollect();

        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS - 1)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS - 2)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS - 3)[0], is(nullValue()));
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 4)[0]).utf8ToString(), is("Austria"));
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 5)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 6)[0]).utf8ToString(), is("USA") );
    }

    @Test
    public void testOrderedDescendingNullsFirstWithoutLimit() throws Exception {
        CrateCollector docCollector = createDocCollector("select \"countryName\" from countries order by 1 desc nulls first", rowReceiver);
        docCollector.doCollect();

        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(rowReceiver.rows.get(0)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(1)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(2)[0], is(nullValue()));
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 1)[0]).utf8ToString(), is("Austria"));
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 2)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 3)[0]).utf8ToString(), is("USA") );
    }

    @Test
    public void testOrderForNonSelected() throws Exception {
        CrateCollector docCollector = createDocCollector("select \"countryName\" from countries order by population desc nulls first", rowReceiver);
        docCollector.doCollect();

        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(rowReceiver.rows.get(0).length, is(2));
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 6)[0]).utf8ToString(), is("USA") );
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS - 5)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS - 4)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS - 3)[0], is(nullValue()));
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 2)[0]).utf8ToString(), is("Austria"));
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 1)[0]).utf8ToString(), is("Germany"));
    }

    @Test
    public void testOrderByScalar() throws Exception {
        CrateCollector docCollector = createDocCollector("select population from countries order by population * -1", rowReceiver);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(((Integer) rowReceiver.rows.get(NUMBER_OF_DOCS - 2)[0]), is(1) );
        assertThat(((Integer) rowReceiver.rows.get(NUMBER_OF_DOCS - 1)[0]), is(0) );
    }

    @Test
    public void testMultiOrdering() throws Exception {
        execute("create table test (x integer, y integer) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        SQLBulkRequest request = new SQLBulkRequest("insert into test values (?, ?)",
                new Object[][]{
                    new Object[]{2, 3},
                    new Object[]{2, 1},
                    new Object[]{2, null},
                    new Object[]{1, null},
                    new Object[]{1, 2},
                    new Object[]{1, 1},
                    new Object[]{1, 0},
                    new Object[]{1, null}
                }
        );
        sqlExecutor.exec(request);
        execute("refresh table test");

        CrateCollector collector = createDocCollector("select x, y from test order by x, y", rowReceiver);
        collector.doCollect();
        collectorProvider.close();

        assertThat(rowReceiver.rows.size(), is(8));

        String expected = "1| 0\n" +
                "1| 1\n" +
                "1| 2\n" +
                "1| NULL\n" +
                "1| NULL\n" +
                "2| 1\n" +
                "2| 3\n" +
                "2| NULL\n";
        assertEquals(expected, printedTable(rowReceiver.result()));

        rowReceiver = new CollectingRowReceiver();

        // Nulls first
        collector = createDocCollector("select x, y from test order by x asc nulls last, y asc nulls first", rowReceiver);
        collector.doCollect();
        collectorProvider.close();

        expected = "1| NULL\n" +
                   "1| NULL\n" +
                   "1| 0\n" +
                   "1| 1\n" +
                   "1| 2\n" +
                   "2| NULL\n" +
                   "2| 1\n" +
                   "2| 3\n";
        assertEquals(expected, printedTable(rowReceiver.result()));
    }

    @Test
    public void testMinScoreQuery() throws Exception {
        CrateCollector docCollector = createDocCollector("select _score from countries where _score >= 1.1", rowReceiver);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(0));
        collectorProvider.close();

        // where _score = 1.0
        rowReceiver.rows.clear();
        docCollector = createDocCollector("select _score from countries where _score >= 1.0", rowReceiver);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
    }

    @Test
    public void testRawExpressionSupportsCompressedSource() throws Exception {
        prepareCreate("test_compressed_source")
                .addMapping("default",
                        "id", "type=integer",
                        "name", "type=string",
                        "_source", "compress=true")
                .execute().actionGet();
        ensureYellow();
        execute("insert into test_compressed_source (id, name) values (?, ?)", new Object[][]{
                {1, "fred"},
                {2, "barney"}
        });
        refresh();

        execute("select _raw from test_compressed_source order by id");
        assertThat(printedTable(response.rows()), is("" +
                "{\"id\":1,\"name\":\"fred\"}\n" +
                "{\"id\":2,\"name\":\"barney\"}\n"));
    }

    @Test
    public void testOrderByFieldVisitorExpressions() throws Exception {
        CrateCollector docCollector = createDocCollector("select _raw, _id from countries order by 1, 2 limit 2", rowReceiver);
        docCollector.doCollect();

        Bucket result = rowReceiver.result();
        assertThat(result.size(), is(2));
        assertThat(printedTable(result), is(
                "{\"continent\":\"America\",\"countryName\":\"USA\",\"population\":1000}| 1000\n" +
                "{\"continent\":\"America\",\"countryName\":\"USA\",\"population\":1001}| 1001\n"));
    }

    private static void addDocToLucene(IndexWriter w, Long value) throws IOException {
        Document doc = new Document();
        if (value != null) {
            doc.add(new LongField("value", value, Field.Store.NO));
        } else {
            // Create a placeholder field
            doc.add(new StringField("null_value", "null", Field.Store.NO));
        }
        w.addDocument(doc);
    }

    private Directory createLuceneIndex() throws IOException {
        File tmpDir = temporaryFolder.newFolder();
        Directory index = FSDirectory.open(tmpDir);
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig cfg = new IndexWriterConfig(Version.LATEST, analyzer);
        IndexWriter w = new IndexWriter(index, cfg);
        for (Long i = 0L; i < 4; i++) {
            if ( i < 2) {
                addDocToLucene(w, i + 1);
            } else {
                addDocToLucene(w, null);
            }
            w.commit();
        }
        w.close();
        return index;
    }

    private TopFieldDocs search(IndexReader reader, Query query, Sort sort) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);

        BooleanQuery searchQuery = new BooleanQuery();
        searchQuery.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        if (query != null) {
            searchQuery.add(query, BooleanClause.Occur.MUST_NOT);
        }
        TopFieldDocs docs = searcher.search(searchQuery, 10, sort);
        return docs;
    }

    private Long[] nextPageQuery(IndexReader reader, FieldDoc lastCollected, boolean reverseFlag, @Nullable Boolean nullFirst) throws IOException {
        OrderBy orderBy = new OrderBy(ImmutableList.<Symbol>of(new Reference(info)),
                new boolean[]{reverseFlag},
                new Boolean[]{nullFirst});

        SortField sortField = new SortField("value", SortField.Type.LONG, reverseFlag);
        Long missingValue = (Long)LuceneMissingValue.missingValue(orderBy, 0);
        sortField.setMissingValue(missingValue);
        Sort sort = new Sort(sortField);

        Query nextPageQuery = OrderedDocCollector.nextPageQuery(lastCollected, orderBy, new Object[]{missingValue});
        TopFieldDocs result = search(reader, nextPageQuery, sort);
        Long results[] = new Long[result.scoreDocs.length];
        for (int i = 0; i < result.scoreDocs.length; i++) {
            Long value = (Long)((FieldDoc)result.scoreDocs[i]).fields[0];
            results[i] = value.equals(missingValue) ? null : value;
        }
        return results;
    }

    private static ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(new TableIdent(null, "table"), "value"), RowGranularity.DOC, DataTypes.LONG);

    // search after queries
    @Test
    public void testSearchAfterQueriesNullsLast() throws Exception {
        Directory index = createLuceneIndex();
        IndexReader reader = DirectoryReader.open(index);

        // reverseOrdering = false, nulls First = false
        // 1  2  null null
        //    ^  (lastCollected = 2)

        FieldDoc afterDoc = new FieldDoc(0, 0, new Object[]{2L});
        Long[] result = nextPageQuery(reader, afterDoc, false, null);
        assertThat(result, is(new Long[]{2L, null, null}));

        // reverseOrdering = false, nulls First = false
        // 1  2  null null
        //       ^
        afterDoc = new FieldDoc(0, 0, new Object[]{LuceneMissingValue.missingValue(false, null, SortField.Type.LONG)});
        result = nextPageQuery(reader, afterDoc, false, null);
        assertThat(result, is(new Long[]{null, null}));

        // reverseOrdering = true, nulls First = false
        // 2  1  null null
        //    ^
        afterDoc = new FieldDoc(0, 0, new Object[]{1L});
        result = nextPageQuery(reader, afterDoc, true, null);
        assertThat(result, is(new Long[]{null, null, 1L}));

        // reverseOrdering = true, nulls First = false
        // 2  1  null null
        //       ^
        afterDoc = new FieldDoc(0, 0, new Object[]{LuceneMissingValue.missingValue(true, null, SortField.Type.LONG)});
        result = nextPageQuery(reader, afterDoc, true, null);
        assertThat(result, is(new Long[]{null, null}));

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
        assertThat(result, is(new Long[]{2L}));

        // reverseOrdering = false, nulls First = true
        // null, null, 1, 2
        //       ^
        afterDoc = new FieldDoc(0, 0, new Object[]{LuceneMissingValue.missingValue(false, true, SortField.Type.LONG)});
        result = nextPageQuery(reader, afterDoc, false, true);
        assertThat(result, is(new Long[]{null, null, 1L, 2L}));

        // reverseOrdering = true, nulls First = true
        // null, null, 2, 1
        //                ^
        afterDoc = new FieldDoc(0, 0, new Object[]{1L});
        result = nextPageQuery(reader, afterDoc, true, true);
        assertThat(result, is(new Long[]{1L}));

        // reverseOrdering = true, nulls First = true
        // null, null, 2, 1
        //       ^
        afterDoc = new FieldDoc(0, 0, new Object[]{LuceneMissingValue.missingValue(true, true, SortField.Type.LONG)});
        result = nextPageQuery(reader, afterDoc, true, true);
        assertThat(result, is(new Long[]{null, null, 2L, 1L}));

        reader.close();
    }
}

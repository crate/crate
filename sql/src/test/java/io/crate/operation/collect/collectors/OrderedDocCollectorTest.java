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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.operation.reference.doc.lucene.LuceneMissingValue;
import io.crate.test.integration.CrateUnitTest;
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
import org.apache.lucene.util.Version;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class OrderedDocCollectorTest extends CrateUnitTest {

    private Directory createLuceneIndex() throws IOException {
        File tmpDir = newTempDir();
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

    private static ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(new TableIdent(null, "table"), "value"), RowGranularity.DOC, DataTypes.LONG);

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
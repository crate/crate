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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.operation.reference.doc.lucene.LuceneMissingValue;
import io.crate.types.DataTypes;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

import static org.hamcrest.core.Is.is;

public class OrderedDocCollectorTest extends RandomizedTest {

    private static final ReferenceInfo INFO = new ReferenceInfo(new ReferenceIdent(new TableIdent(null, "table"), "value"), RowGranularity.DOC, DataTypes.LONG);

    private Directory createLuceneIndex() throws IOException {
        File tmpDir = newTempDir();
        Directory index = FSDirectory.open(tmpDir.toPath());
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig cfg = new IndexWriterConfig(analyzer);
        IndexWriter w = new IndexWriter(index, cfg);
        for (Long i = 0L; i < 4; i++) {
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

    private static void addDocToLucene(IndexWriter w, Long value) throws IOException {
        Document doc = new Document();
        if (value != null) {
            MappedFieldType fieldType = new LongFieldMapper.LongFieldType();
            fieldType.setNames(new MappedFieldType.Names("value"));
            doc.add(new LongFieldMapper.CustomLongNumericField(value, fieldType));
            doc.add(new SortedNumericDocValuesField("value", value));
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

    private Long[] nextPageQuery(IndexReader reader, FieldDoc lastCollected, boolean reverseFlag, @Nullable Boolean nullFirst) throws IOException {
        OrderBy orderBy = new OrderBy(ImmutableList.<Symbol>of(new Reference(INFO)),
                new boolean[]{reverseFlag},
                new Boolean[]{nullFirst});

        SortField sortField = new SortedNumericSortField("value", SortField.Type.LONG, reverseFlag);
        Long missingValue = (Long) LuceneMissingValue.missingValue(orderBy, 0);
        sortField.setMissingValue(missingValue);
        Sort sort = new Sort(sortField);

        Query nextPageQuery = OrderedDocCollector.nextPageQuery(lastCollected, orderBy, new Object[]{missingValue});
        TopFieldDocs result = search(reader, nextPageQuery, sort);
        Long results[] = new Long[result.scoreDocs.length];
        for (int i = 0; i < result.scoreDocs.length; i++) {
            Long value = (Long) ((FieldDoc) result.scoreDocs[i]).fields[0];
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
        result = nextPageQuery(reader, afterDoc, true, false);
        assertThat(result, is(new Long[]{1L, null, null}));

        // reverseOrdering = true, nulls First = false
        // 2  1  null null
        //       ^
        afterDoc = new FieldDoc(0, 0, new Object[]{LuceneMissingValue.missingValue(true, false, SortField.Type.LONG)});
        result = nextPageQuery(reader, afterDoc, true, false);
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
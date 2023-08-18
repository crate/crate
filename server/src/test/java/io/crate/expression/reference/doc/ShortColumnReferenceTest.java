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

package io.crate.expression.reference.doc;

import static io.crate.testing.Asserts.assertThat;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.reference.doc.lucene.ShortColumnReference;

public class ShortColumnReferenceTest extends DocLevelExpressionsTest {

    private static final String COLUMN = "s";

    public ShortColumnReferenceTest() {
        super("create table t (l short)");
    }

    @Override
    protected void insertValues(IndexWriter writer) throws Exception {
        for (short i = -10; i < 10; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", Short.toString(i), Field.Store.NO));
            doc.add(new SortedNumericDocValuesField(COLUMN, i));
            writer.addDocument(doc);
        }
    }

    @Test
    public void testShortExpression() throws Exception {
        ShortColumnReference shortColumn = new ShortColumnReference(COLUMN);
        shortColumn.startCollect(ctx);
        shortColumn.setNextReader(new ReaderContext(readerContext));
        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 20);
        short i = -10;
        for (ScoreDoc doc : topDocs.scoreDocs) {
            shortColumn.setNextDocId(doc.doc);
            assertThat(shortColumn.value()).isEqualTo(i);
            i++;
        }
    }
}

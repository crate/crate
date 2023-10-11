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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.reference.doc.lucene.BooleanColumnReference;

public class BooleanColumnReferenceTest extends DocLevelExpressionsTest {

    private static final String COLUMN = "b";

    public BooleanColumnReferenceTest() {
        super("create table t (b boolean)");
    }

    @Override
    protected void insertValues(IndexWriter writer) throws Exception {
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", Integer.toString(i), Field.Store.NO));
            doc.add(new NumericDocValuesField(COLUMN, i % 2 == 0 ? 1 : 0));
            writer.addDocument(doc);
        }
    }

    @Test
    public void testBooleanExpression() throws Exception {
        BooleanColumnReference booleanColumn = new BooleanColumnReference(COLUMN);
        booleanColumn.startCollect(ctx);
        booleanColumn.setNextReader(new ReaderContext(readerContext));
        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 20);
        int i = 0;
        for (ScoreDoc doc : topDocs.scoreDocs) {
            booleanColumn.setNextDocId(doc.doc);
            assertThat(booleanColumn.value()). isEqualTo(i % 2 == 0);
            i++;
        }
    }
}

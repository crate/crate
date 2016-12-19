/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.doc;

import io.crate.operation.reference.doc.lucene.DoubleColumnReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LegacyDoubleField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class DoubleColumnReferenceTest extends DocLevelExpressionsTest {

    @Override
    protected void insertValues(IndexWriter writer) throws Exception {
        for (double d = 0.5; d < 10.0d; d++) {
            Document doc = new Document();
            doc.add(new StringField("_id", Double.toString(d), Field.Store.NO));
            doc.add(new LegacyDoubleField(fieldName(), d, Field.Store.NO));
            writer.addDocument(doc);
        }
    }

    @Override
    protected String fieldName() {
        return "d";
    }

    @Test
    public void testFieldCacheExpression() throws Exception {
        DoubleColumnReference doubleColumn = new DoubleColumnReference(fieldName());
        doubleColumn.startCollect(ctx);
        doubleColumn.setNextReader(readerContext);
        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
        double d = 0.5;
        for (ScoreDoc doc : topDocs.scoreDocs) {
            doubleColumn.setNextDocId(doc.doc);
            assertThat(doubleColumn.value(), is(d));
            d++;
        }
    }
}

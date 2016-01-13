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

import io.crate.operation.reference.doc.lucene.ShortColumnReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class ShortColumnReferenceTest extends DocLevelExpressionsTest {
    @Override
    protected void insertValues(IndexWriter writer) throws Exception {
        for (short i = -10; i<10; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", Short.toString(i), Field.Store.NO));
            // TODO: FIX ME! is indexName proper replacement of name()?
            doc.add(new IntField(fieldName().indexName(), i, Field.Store.NO));
            writer.addDocument(doc);
        }
    }

    @Override
    protected MappedFieldType.Names fieldName() {
        return new MappedFieldType.Names("s");
    }

    @Override
    protected FieldDataType fieldType() {
        return new FieldDataType("short");
    }

    @Test
    public void testFieldCacheExpression() throws Exception {
        // TODO: FIX ME! is indexName proper replacement of name()?
        ShortColumnReference shortColumn = new ShortColumnReference(fieldName().indexName());
        shortColumn.startCollect(ctx);
        shortColumn.setNextReader(readerContext);
        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 20);
        short i = -10;
        for (ScoreDoc doc : topDocs.scoreDocs) {
            shortColumn.setNextDocId(doc.doc);
            assertThat(shortColumn.value(), is(i));
            i++;
        }
    }
}

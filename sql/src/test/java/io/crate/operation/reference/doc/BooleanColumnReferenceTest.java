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

import io.crate.operation.reference.doc.lucene.BooleanColumnReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class BooleanColumnReferenceTest extends DocLevelExpressionsTest {

    private static final BytesRef TRUE = new BytesRef("1");
    private static final BytesRef FALSE = new BytesRef("0");

    @Override
    protected void insertValues(IndexWriter writer) throws Exception {
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", Integer.toString(i), Field.Store.NO));
            doc.add(new StringField(columnName(),
                (i % 2 == 0 ? TRUE : FALSE).utf8ToString(), Field.Store.NO));
            writer.addDocument(doc);
        }
    }

    @Test
    public void testFieldCacheExpression() throws Exception {
        MappedFieldType fieldType = StringFieldMapper.Defaults.FIELD_TYPE.clone();
        fieldType.setNames(fieldName());
        BooleanColumnReference booleanColumn = new BooleanColumnReference(columnName(), fieldType);
        booleanColumn.startCollect(ctx);
        booleanColumn.setNextReader(readerContext);
        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 20);
        int i = 0;
        for (ScoreDoc doc : topDocs.scoreDocs) {
            booleanColumn.setNextDocId(doc.doc);
            assertThat(booleanColumn.value(), is(i % 2 == 0));
            i++;
        }
    }

    @Override
    protected String columnName() {
        return "b";
    }
}

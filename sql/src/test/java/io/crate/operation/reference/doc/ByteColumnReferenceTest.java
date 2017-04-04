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

import io.crate.operation.reference.doc.lucene.ByteColumnReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.ByteFieldMapper;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class ByteColumnReferenceTest extends DocLevelExpressionsTest {

    @Override
    protected void insertValues(IndexWriter writer) throws Exception {
        for (byte b = -10; b < 10; b++) {
            Document doc = new Document();
            doc.add(new StringField("_id", Byte.toString(b), Field.Store.NO));
            doc.add(new IntField(fieldName().indexName(), b, Field.Store.NO));
            writer.addDocument(doc);
        }
    }

    @Test
    public void testFieldCacheExpression() throws Exception {
        MappedFieldType fieldType = ByteFieldMapper.Defaults.FIELD_TYPE.clone();
        fieldType.setNames(fieldName());
        ByteColumnReference byteColumn = new ByteColumnReference(columnName(), fieldType);
        byteColumn.startCollect(ctx);
        byteColumn.setNextReader(readerContext);
        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 20);
        byte b = -10;
        for (ScoreDoc doc : topDocs.scoreDocs) {
            byteColumn.setNextDocId(doc.doc);
            assertThat(byteColumn.value(), is(b));
            b++;
        }
    }

    @Override
    protected String columnName() {
        return "b";
    }
}

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

package io.crate.operation.reference.doc.lucene;

import io.crate.operation.reference.doc.DocLevelExpressionsTest;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TokenCountCollectorExpressionTest extends DocLevelExpressionsTest {

    @Override
    protected void insertValues(IndexWriter writer) throws Exception {
        Document doc = new Document();
        doc.add(new StringField(fieldName().name(), "bar1", Field.Store.NO));
        doc.add(new StringField(fieldName().name(), "bar2", Field.Store.NO));
        writer.addDocument(doc);
    }

    @Override
    protected FieldMapper.Names fieldName() {
        return new FieldMapper.Names("foo");
    }

    @Override
    protected FieldDataType fieldType() {
        return new FieldDataType("string");
    }

    @Test
    public void testTokenCountExpression() throws Exception {
        TokenCountCollectorExpression expression = new TokenCountCollectorExpression(fieldName().name());
        expression.startCollect(ctx);
        expression.setNextReader(readerContext);
        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 20);

        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            expression.setNextDocId(scoreDoc.doc);
            assertThat(expression.value(), is(2));
        }
    }
}
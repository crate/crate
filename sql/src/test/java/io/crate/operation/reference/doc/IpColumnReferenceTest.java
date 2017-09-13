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

package io.crate.operation.reference.doc;

import io.crate.exceptions.GroupByOnArrayUnsupportedException;
import io.crate.operation.reference.doc.lucene.IpColumnReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetAddress;

import static org.hamcrest.core.Is.is;

public class IpColumnReferenceTest extends DocLevelExpressionsTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private String column = "i";
    private String column_array = "ia";

    @Override
    protected void insertValues(IndexWriter writer) throws Exception {
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            InetAddress address = InetAddresses.forString("192.168.0." + i);
            doc.add(new SortedSetDocValuesField(column, new BytesRef(InetAddressPoint.encode(address))));
            if (i == 0) {
                address = InetAddresses.forString("192.168.0.1");
                doc.add(new SortedSetDocValuesField(column_array, new BytesRef(InetAddressPoint.encode(address))));
                address = InetAddresses.forString("192.168.0.2");
                doc.add(new SortedSetDocValuesField(column_array, new BytesRef(InetAddressPoint.encode(address))));
            }
            writer.addDocument(doc);
        }
        for (int i = 10; i < 20; i++) {
            Document doc = new Document();
            InetAddress address = InetAddresses.forString("7bd0:8082:2df8:487e:e0df:e7b5:9362:" + Integer.toHexString(i));
            doc.add(new SortedSetDocValuesField(column, new BytesRef(InetAddressPoint.encode(address))));
            writer.addDocument(doc);
        }
    }

    @Test
    public void testIpExpression() throws Exception {
        IpColumnReference columnReference = new IpColumnReference(column);
        columnReference.startCollect(ctx);
        columnReference.setNextReader(readerContext);
        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 20);
        int i = 0;
        for (ScoreDoc doc : topDocs.scoreDocs) {
            columnReference.setNextDocId(doc.doc);
            if (i < 10) {
                assertThat(columnReference.value(), is(new BytesRef("192.168.0." + i)));
            } else {
                assertThat(columnReference.value(), is(new BytesRef("7bd0:8082:2df8:487e:e0df:e7b5:9362:" + Integer.toHexString(i))));
            }
            i++;
        }
    }

    @Test
    public void testIpExpressionOnArrayThrowsException() throws Exception {
        IpColumnReference columnReference = new IpColumnReference(column_array);
        columnReference.startCollect(ctx);
        columnReference.setNextReader(readerContext);
        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 20);

        ScoreDoc doc = topDocs.scoreDocs[0];

        expectedException.expect(GroupByOnArrayUnsupportedException.class);
        expectedException.expectMessage("Column \"ia\" has a value that is an array. Group by doesn't work on Arrays");

        columnReference.setNextDocId(doc.doc);
    }

}

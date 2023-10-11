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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.junit.Test;

import io.crate.exceptions.ArrayViaDocValuesUnsupportedException;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.reference.doc.lucene.IpColumnReference;

public class IpColumnReferenceTest extends DocLevelExpressionsTest {

    private static final String IP_COLUMN = "i";
    private static final String IP_ARRAY_COLUMN = "ia";

    public IpColumnReferenceTest() {
        super("create table t (i ip, ia array(ip))");
    }

    @Override
    protected void insertValues(IndexWriter writer) throws Exception {
        addIPv4Values(writer);
        addIPv6Values(writer);

        // Doc without IP_COLUMN field to simulate NULL value
        Document doc = new Document();
        doc.add(new StringField("_id", Integer.toString(20), Field.Store.NO));
        writer.addDocument(doc);
    }

    private static void addIPv6Values(IndexWriter writer) throws IOException {
        for (int i = 10; i < 20; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", Integer.toString(i), Field.Store.NO));
            InetAddress address = InetAddresses.forString("7bd0:8082:2df8:487e:e0df:e7b5:9362:" + Integer.toHexString(i));
            doc.add(new SortedSetDocValuesField(IP_COLUMN, new BytesRef(InetAddressPoint.encode(address))));
            writer.addDocument(doc);
        }
    }

    private static void addIPv4Values(IndexWriter writer) throws IOException {
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            doc.add(new StringField("_id", Integer.toString(i), Field.Store.NO));
            InetAddress address = InetAddresses.forString("192.168.0." + i);
            doc.add(new SortedSetDocValuesField(IP_COLUMN, new BytesRef(InetAddressPoint.encode(address))));
            if (i == 0) {
                address = InetAddresses.forString("192.168.0.1");
                doc.add(new SortedSetDocValuesField(IP_ARRAY_COLUMN, new BytesRef(InetAddressPoint.encode(address))));
                address = InetAddresses.forString("192.168.0.2");
                doc.add(new SortedSetDocValuesField(IP_ARRAY_COLUMN, new BytesRef(InetAddressPoint.encode(address))));
            }
            writer.addDocument(doc);
        }
    }

    @Test
    public void testIpExpression() throws Exception {
        IpColumnReference columnReference = new IpColumnReference(IP_COLUMN);
        columnReference.startCollect(ctx);
        columnReference.setNextReader(new ReaderContext(readerContext));
        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 21);
        assertThat(topDocs.scoreDocs.length).isEqualTo(21);

        int i = 0;
        for (ScoreDoc doc : topDocs.scoreDocs) {
            columnReference.setNextDocId(doc.doc);
            if (i == 20) {
                assertThat(columnReference.value()).isNull();
            } else if (i < 10) {
                assertThat(columnReference.value()).isEqualTo("192.168.0." + i);
            } else {
                assertThat(columnReference.value()).isEqualTo(
                    "7bd0:8082:2df8:487e:e0df:e7b5:9362:" + Integer.toHexString(i));
            }
            i++;
        }
    }

    @Test
    public void testIpExpressionOnArrayThrowsException() throws Exception {
        IpColumnReference columnReference = new IpColumnReference(IP_ARRAY_COLUMN);
        columnReference.startCollect(ctx);
        columnReference.setNextReader(new ReaderContext(readerContext));
        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);

        ScoreDoc doc = topDocs.scoreDocs[0];
        columnReference.setNextDocId(doc.doc);

        assertThatThrownBy(() -> columnReference.value())
            .isExactlyInstanceOf(ArrayViaDocValuesUnsupportedException.class)
            .hasMessage("Column \"ia\" has a value that is an array. Loading arrays via doc-values is not supported.");
    }

    @Test
    public void testNullDocValuesDoNotResultInNPE() throws IOException {
        IpColumnReference ref = new IpColumnReference("missing_column");
        ref.startCollect(ctx);
        ref.setNextReader(new ReaderContext(readerContext));
        ref.setNextDocId(0);

        assertThat(ref.value()).isNull();
    }
}

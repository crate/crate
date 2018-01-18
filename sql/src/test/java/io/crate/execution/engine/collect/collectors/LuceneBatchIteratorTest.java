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

package io.crate.execution.engine.collect.collectors;

import io.crate.breaker.RamAccountingContext;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LongColumnReference;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.BatchIteratorTester;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LuceneBatchIteratorTest extends CrateUnitTest {

    private List<LongColumnReference> columnRefs;
    private IndexSearcher indexSearcher;
    private ArrayList<Object[]> expectedResult;

    @Before
    public void prepareSearcher() throws Exception {
        IndexWriter iw = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new StandardAnalyzer()));
        String columnName = "x";
        expectedResult = new ArrayList<>(20);
        for (long i = 0; i < 20; i++) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField(columnName, i));
            iw.addDocument(doc);
            expectedResult.add(new Object[] { i });
        }
        iw.commit();
        indexSearcher = new IndexSearcher(DirectoryReader.open(iw));
        LongColumnReference columnReference = new LongColumnReference(columnName);
        columnRefs = Collections.singletonList(columnReference);
    }

    @Test
    public void testLuceneBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> {
                return new LuceneBatchIterator(
                    indexSearcher,
                    new MatchAllDocsQuery(),
                    null,
                    false,
                    new CollectorContext(
                        mappedFieldType -> null,
                        new CollectorFieldsVisitor(0)
                    ),
                    new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy")),
                    columnRefs,
                    columnRefs
                );
            }
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }
}

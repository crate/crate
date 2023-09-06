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

import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.Version;
import org.junit.After;
import org.junit.Before;

import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.IndexEnv;
import io.crate.testing.SQLExecutor;

public abstract class DocLevelExpressionsTest extends CrateDummyClusterServiceUnitTest {

    private final String createTableStatement;
    protected CollectorContext ctx;
    LeafReaderContext readerContext;
    private IndexEnv indexEnv;

    protected DocLevelExpressionsTest(String createTableStatement) {
        this.createTableStatement = createTableStatement;
    }

    @Before
    public void prepare() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable(createTableStatement)
            .build();
        indexEnv = new IndexEnv(
            THREAD_POOL,
            (DocTableInfo) StreamSupport.stream(e.schemas().spliterator(), false)
                .filter(x -> x instanceof DocSchemaInfo)
                .map(x -> (DocSchemaInfo) x)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No doc schema found"))
                .getTables()
                .iterator()
                .next(),
            clusterService.state(),
            Version.CURRENT,
            createTempDir()
        );
        IndexWriter writer = indexEnv.writer();
        insertValues(writer);
        DirectoryReader directoryReader = DirectoryReader.open(writer, true, true);
        readerContext = directoryReader.leaves().get(0);
        ctx = new CollectorContext(Set.of(), Function.identity());
    }

    @After
    public void cleanUp() throws Exception {
        indexEnv.close();
    }

    protected abstract void insertValues(IndexWriter writer) throws Exception;
}

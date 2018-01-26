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

package io.crate.expression.reference.doc;

import io.crate.expression.reference.doc.lucene.CollectorContext;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

public abstract class DocLevelExpressionsTest extends ESSingleNodeTestCase {

    protected CollectorContext ctx;
    protected IndexFieldDataService ifd;
    protected LeafReaderContext readerContext;
    private IndexWriter writer;

    @Before
    public void prepare() throws Exception {
        Settings settings = Settings.builder().put("index.fielddata.cache", "none").build();
        IndexService indexService = createIndex("test", settings);
        ifd = indexService.fieldData();
        writer = new IndexWriter(new RAMDirectory(),
            new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy()));

        insertValues(writer);

        DirectoryReader directoryReader = DirectoryReader.open(writer, true, true);
        readerContext = directoryReader.leaves().get(0);

        ctx = new CollectorContext(ifd, null);
    }

    @After
    public void cleanUp() throws Exception {
        writer.close();
        writer.getDirectory().close();
        ifd.clear();
    }

    protected abstract void insertValues(IndexWriter writer) throws Exception;
}

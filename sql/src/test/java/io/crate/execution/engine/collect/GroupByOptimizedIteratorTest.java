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

package io.crate.execution.engine.collect;

import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class GroupByOptimizedIteratorTest extends CrateUnitTest {

    private IndexWriter iw;

    @Before
    public void setupIndexWriter() throws Exception {
        MMapDirectory directory = new MMapDirectory(createTempDir());
        iw = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()));
    }

    @After
    public void tearDownIndexWriter() throws Exception {
        iw.close();
        iw.getDirectory().close();
    }

    @Test
    public void testHighCardinalityRatioReturnsTrueForHighCardinality() throws Exception {
        String columnName = "x";
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            BytesRef value = new BytesRef(Integer.toString(i));
            doc.add(new Field(columnName, value, KeywordFieldMapper.Defaults.FIELD_TYPE.clone()));
            iw.addDocument(doc);
        }
        iw.commit();

        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(iw));
        assertThat(
            GroupByOptimizedIterator.hasHighCardinalityRatio(() -> new Engine.Searcher("dummy", indexSearcher, () -> {}), "x"),
            is(true)
        );
    }

    @Test
    public void testHighCardinalityRatioReturnsTrueForLowCardinality() throws Exception {
        String columnName = "x";
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            BytesRef value = new BytesRef("1");
            doc.add(new Field(columnName, value, KeywordFieldMapper.Defaults.FIELD_TYPE.clone()));
            iw.addDocument(doc);
        }
        iw.commit();

        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(iw));
        assertThat(
            GroupByOptimizedIterator.hasHighCardinalityRatio(() -> new Engine.Searcher("dummy", indexSearcher, () -> {}), "x"),
            is(false)
        );
    }
}

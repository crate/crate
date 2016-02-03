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

import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.test.integration.CrateSingleNodeTest;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.junit.After;
import org.junit.Before;
import org.mockito.Matchers;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class DocLevelExpressionsTest extends CrateSingleNodeTest {

    protected CollectorContext ctx;
    protected IndexFieldDataService ifd;
    protected LeafReaderContext readerContext;
    private IndexWriter writer;

    @Before
    public void prepare() throws Exception {
        Settings settings = Settings.builder().put("index.fielddata.cache", "none").build();
        IndexService indexService = createIndex("test", settings);
        ifd = indexService.fieldData();

        MapperService mapperService = mock(MapperService.class);
        MappedFieldType fieldMapper = mock(MappedFieldType.class);
        when(fieldMapper.names()).thenReturn(fieldName());
        when(fieldMapper.fieldDataType()).thenReturn(fieldType());
        // TODO: FIX ME! smart mappers not available anymore
        //when(mapperService.smartNameFieldMapper(anyString(), Matchers.<String[]>any())).thenReturn(fieldMapper);


        IndexFieldData<?> fieldData = ifd.getForField(fieldMapper);
        writer = new IndexWriter(new RAMDirectory(),
                new IndexWriterConfig(new StandardAnalyzer())
                        .setMergePolicy(new LogByteSizeMergePolicy()));

        insertValues(writer);

        DirectoryReader directoryReader = DirectoryReader.open(writer, true);
        readerContext = directoryReader.leaves().get(0);
        fieldData.load(readerContext);

        ctx = new CollectorContext(mapperService, ifd, null);
    }

    @After
    public void cleanUp() throws Exception {
        writer.close();
        writer.getDirectory().close();
        ifd.clear();
    }

    protected abstract void insertValues(IndexWriter writer) throws Exception;

    protected abstract MappedFieldType.Names fieldName();

    protected abstract FieldDataType fieldType();
}

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

package io.crate.operator.reference.doc;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.cratedb.Constants;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.junit.After;
import org.junit.Before;
import org.mockito.Matchers;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class DocLevelExpressionsTest {

    protected CollectorContext ctx;
    protected IndexFieldDataService ifd;
    protected AtomicReaderContext readerContext;
    private IndexWriter writer;

    @Before
    public void prepare() throws Exception {
        ifd = new IndexFieldDataService(new Index("test"));

        IndexFieldData<?> fieldData = ifd.getForField(fieldName(), fieldType());
        writer = new IndexWriter(new RAMDirectory(),
                new IndexWriterConfig(Lucene.VERSION, new StandardAnalyzer(Lucene.VERSION))
                        .setMergePolicy(new LogByteSizeMergePolicy()));

        insertValues(writer);
        AtomicReader reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(writer, true));
        readerContext = reader.getContext();
        fieldData.load(readerContext);

        ShardSearchRequest request = new ShardSearchRequest();
        request.types(new String[]{Constants.DEFAULT_MAPPING_TYPE});

        MapperService mapperService = mock(MapperService.class);
        FieldMapper fieldMapper = mock(FieldMapper.class);
        when(fieldMapper.names()).thenReturn(fieldName());
        when(fieldMapper.fieldDataType()).thenReturn(fieldType());
        when(mapperService.smartNameFieldMapper(anyString(), Matchers.<String[]>any())).thenReturn(fieldMapper);

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(searchContext.fieldData()).thenReturn(ifd);
        ctx = new CollectorContext().searchContext(searchContext);
    }

    @After
    public void cleanUp() throws Exception {
        writer.close();
        writer.getDirectory().close();
        ifd.clear();
    }

    protected abstract void insertValues(IndexWriter writer) throws Exception;

    protected abstract FieldMapper.Names fieldName();

    protected abstract FieldDataType fieldType();
}

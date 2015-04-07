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

package io.crate.analyze;

import io.crate.metadata.MetaDataModule;
import io.crate.metadata.PartitionName;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.testing.MockedClusterServiceModule;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Module;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RefreshAnalyzerTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    private final static TableIdent TEST_BLOB_TABLE_IDENT = new TableIdent("blob", "blobs");

    static class TestMetaDataModule extends MetaDataModule {

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            BlobTableInfo blobTableInfo = mock(BlobTableInfo.class);
            when(blobTableInfo.ident()).thenReturn(TEST_BLOB_TABLE_IDENT);
            when(blobTableInfo.schemaInfo()).thenReturn(schemaInfo);
            when(schemaInfo.getTableInfo(TEST_BLOB_TABLE_IDENT.name())).thenReturn(blobTableInfo);
            schemaBinder.addBinding(BlobSchemaInfo.NAME).toInstance(schemaInfo);

            SchemaInfo docSchemaInfo = mock(SchemaInfo.class);
            when(docSchemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name()))
                    .thenReturn(TEST_PARTITIONED_TABLE_INFO);
            when(docSchemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);

            schemaBinder.addBinding(ReferenceInfos.DEFAULT_SCHEMA_NAME).toInstance(docSchemaInfo);
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new MockedClusterServiceModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule()
        ));
        return modules;
    }

    @Test
    public void testRefreshSystemTable() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        analyze("refresh table sys.shards");
    }

    @Test
    public void testRefreshBlobTable() throws Exception {
        RefreshTableAnalyzedStatement analysis = (RefreshTableAnalyzedStatement)analyze("refresh table blob.blobs");
        assertThat(analysis.table().ident().schema(), is("blob"));
        assertThat(analysis.table().ident().name(), is("blobs"));

    }

    @Test
    public void testRefreshPartition() throws Exception {
        PartitionName partition = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000")));
        RefreshTableAnalyzedStatement analysis = (RefreshTableAnalyzedStatement)analyze("refresh table parted PARTITION (date=1395874800000)");
        assertThat(analysis.table().ident().name(), is("parted"));
        assertThat(analysis.partitionName().stringValue(), is(partition.stringValue()));
    }

    @Test
    public void testRefreshPartitionsParameter() throws Exception {
        PartitionName partition = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000")));
        RefreshTableAnalyzedStatement analysis = (RefreshTableAnalyzedStatement) analyze(
                "refresh table parted PARTITION (date=?)", new Object[] {"1395874800000"});
        assertThat(analysis.table().ident().name(), is("parted"));
        assertThat(analysis.partitionName().stringValue(), is(partition.stringValue()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRefreshInvalidPartitioned() throws Exception {
        analyze("refresh table parted partition (invalid_column='hddsGNJHSGFEFZÜ')");
    }

    @Test
    public void testRefreshNonPartitioned() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        analyze("refresh table users partition (foo='n')");
    }

    @Test
    public void testRefreshSysPartitioned() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        analyze("refresh table sys.shards partition (id='n')");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRefreshBlobPartitioned() throws Exception {
        analyze("refresh table blob.blobs partition (n='n')");
    }

    @Test
    public void testRefreshPartitionedTableNullPartition() throws Exception {
        RefreshTableAnalyzedStatement analysis = (RefreshTableAnalyzedStatement) analyze("refresh table parted PARTITION (date=null)");
        assertNotNull(analysis.partitionName());
        assertThat(analysis.partitionName().stringValue(), is(".partitioned.parted.0400"));
    }
}

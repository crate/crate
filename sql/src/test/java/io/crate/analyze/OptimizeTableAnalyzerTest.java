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

package io.crate.analyze;

import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OptimizeTableAnalyzerTest extends BaseAnalyzerTest {

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
            when(schemaInfo.getTableInfo(TEST_BLOB_TABLE_IDENT.name())).thenReturn(blobTableInfo);
            schemaBinder.addBinding(BlobSchemaInfo.NAME).toInstance(schemaInfo);

            SchemaInfo docSchemaInfo = mock(SchemaInfo.class);
            when(docSchemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name()))
                .thenReturn(TEST_PARTITIONED_TABLE_INFO);
            when(docSchemaInfo.getTableInfo(USER_TABLE_IDENT.name())).thenReturn(USER_TABLE_INFO);

            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(docSchemaInfo);
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
    public void testOptimizeSystemTable() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("operation cannot be performed on system and blob tables: table 'sys.shards'");
        analyze("OPTIMIZE TABLE sys.shards");
    }

    @Test
    public void testOptimizeBlobTable() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("operation cannot be performed on system and blob tables: table 'blob.blobs'");
        analyze("OPTIMIZE TABLE blob.blobs");
    }

    @Test
    public void testOptimizeTable() throws Exception {
        OptimizeTableAnalyzedStatement analysis = analyze("OPTIMIZE TABLE users");
        assertThat(analysis.indexNames().size(), is(1));
        assertThat(analysis.indexNames(), hasItem("users"));
    }

    @Test
    public void testOptimizeTableWithParams() throws Exception {
        OptimizeTableAnalyzedStatement analysis = analyze("OPTIMIZE TABLE users WITH (max_num_segments=2)");
        assertThat(analysis.indexNames().size(), is(1));
        assertThat(analysis.indexNames(), hasItem("users"));
        assertThat(analysis.settings().getAsInt(OptimizeSettings.MAX_NUM_SEGMENTS.name(), null), is(2));
        analysis = analyze("OPTIMIZE TABLE users WITH (only_expunge_deletes=true)");
        assertThat(analysis.indexNames().size(), is(1));
        assertThat(analysis.indexNames(), hasItem("users"));
        assertThat(analysis.settings().getAsBoolean(OptimizeSettings.ONLY_EXPUNGE_DELETES.name(), null), is(Boolean.TRUE));
        analysis = analyze("OPTIMIZE TABLE users WITH (flush=false)");
        assertThat(analysis.indexNames().size(), is(1));
        assertThat(analysis.indexNames(), hasItem("users"));
        assertThat(analysis.settings().getAsBoolean(OptimizeSettings.FLUSH.name(), null), is(Boolean.FALSE));
    }

    @Test
    public void testOptimizeTableWithInvalidParamName() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'invalidparam' not supported");
        analyze("OPTIMIZE TABLE users WITH (invalidParam=123)");
    }

    @Test
    public void testOptimizePartition() throws Exception {
        OptimizeTableAnalyzedStatement analysis = analyze("OPTIMIZE TABLE parted PARTITION (date=1395874800000)");
        assertThat(analysis.indexNames(), hasItem(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
    }

    @Test
    public void testOptimizePartitionedTableNullPartition() throws Exception {
        OptimizeTableAnalyzedStatement analysis = analyze("OPTIMIZE TABLE parted PARTITION (date=null)");
        assertThat(analysis.indexNames(), contains(Matchers.hasToString(".partitioned.parted.0400"))
        );
    }

    @Test
    public void testOptimizePartitionWithParams() throws Exception {
        OptimizeTableAnalyzedStatement analysis = analyze("OPTIMIZE TABLE parted PARTITION (date=1395874800000) " +
                                                          "WITH (only_expunge_deletes=true)");
        assertThat(analysis.indexNames(), hasItem(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
    }

    @Test
    public void testOptimizeMultipleTables() throws Exception {
        OptimizeTableAnalyzedStatement analysis = analyze("OPTIMIZE TABLE parted, users");
        assertThat(analysis.indexNames().size(), is(4));
        assertThat(analysis.indexNames(), hasItem(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
        assertThat(analysis.indexNames(), hasItem("users"));
    }

    @Test
    public void testOptimizeMultipleTablesUnknown() throws Exception {
        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage("Table 'doc.foo' unknown");
        analyze("OPTIMIZE TABLE parted, foo, bar");
    }

    @Test
    public void testOptimizeInvalidPartitioned() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"invalid_column\" is no known partition column");
        analyze("OPTIMIZE TABLE parted PARTITION (invalid_column='hddsGNJHSGFEFZÜ')");
    }

    @Test
    public void testOptimizeNonPartitioned() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("table 'doc.users' is not partitioned");
        analyze("OPTIMIZE TABLE users PARTITION (foo='n')");
    }

    @Test
    public void testOptimizeSysPartitioned() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("operation cannot be performed on system and blob tables: table 'sys.shards'");
        analyze("OPTIMIZE TABLE sys.shards PARTITION (id='n')");
    }

    @Test
    public void testOptimizeBlobPartitioned() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("operation cannot be performed on system and blob tables: table 'blob.blobs'");
        analyze("OPTIMIZE TABLE blob.blobs partition (n='n')");
    }
}

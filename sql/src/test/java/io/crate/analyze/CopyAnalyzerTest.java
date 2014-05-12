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

import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.planner.symbol.Parameter;
import io.crate.planner.symbol.Reference;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CopyAnalyzerTest extends BaseAnalyzerTest {

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);
            when(schemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name())).thenReturn(TEST_PARTITIONED_TABLE_INFO);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new TestModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new OperatorModule())
        );
        return modules;
    }

    @Test
    public void testCopyFromExistingTable() throws Exception {
        CopyAnalysis analysis = (CopyAnalysis)analyze("copy users from '/some/distant/file.ext'");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertLiteralSymbol(analysis.uri(), "/some/distant/file.ext");
    }

    @Test
    public void testCopyFromExistingPartitionedTable() throws Exception {
        CopyAnalysis analysis = (CopyAnalysis)analyze("copy parted from '/some/distant/file.ext'");
        assertThat(analysis.table().ident(), is(TEST_PARTITIONED_TABLE_IDENT));
        assertLiteralSymbol(analysis.uri(), "/some/distant/file.ext");
    }

    @Test( expected = TableUnknownException.class)
    public void testCopyFromNonExistingTable() throws Exception {
        analyze("copy unknown from '/some/distant/file.ext'");
    }

    @Test( expected = UnsupportedOperationException.class)
    public void testCopyFromSystemTable() throws Exception {
        analyze("copy sys.shards from '/nope/nope/still.nope'");
    }

    @Test( expected = SchemaUnknownException.class)
    public void testCopyFromUnknownSchema() throws Exception {
        analyze("copy suess.shards from '/nope/nope/still.nope'");
    }

    @Test
    public void testCopyFromParameter() throws Exception {
        String path = "/some/distant/file.ext";
        CopyAnalysis analysis = (CopyAnalysis)analyze("copy users from ?", new Object[]{path});
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat((String)((Parameter)analysis.uri()).value(), is(path));
    }

    @Test
    public void testCopyToFile() throws Exception {
        CopyAnalysis analysis = (CopyAnalysis)analyze("copy users to '/blah.txt'");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.mode(), is(CopyAnalysis.Mode.TO));
        assertLiteralSymbol(analysis.uri(), "/blah.txt");
    }

    @Test
    public void testCopyToDirectory() throws Exception {
        CopyAnalysis analysis = (CopyAnalysis)analyze("copy users to directory '/foo'");
        assertThat(analysis.directoryUri(), is(true));
    }

    @Test
    public void testCopyToWithColumnList() throws Exception {
        CopyAnalysis analysis = (CopyAnalysis)analyze("copy users (id, name) to DIRECTORY '/tmp'");
        assertThat(analysis.outputSymbols().size(), is(2));
        assertThat(((Reference)analysis.outputSymbols().get(0)).info().ident().columnIdent().name(), is("id"));
        assertThat(((Reference)analysis.outputSymbols().get(1)).info().ident().columnIdent().name(), is("name"));
    }

    @Test
    public void testCopyToFileWithParams() throws Exception {
        CopyAnalysis analysis = (CopyAnalysis)analyze("copy users to '/blah.txt' with (compression='gzip')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.mode(), is(CopyAnalysis.Mode.TO));

        assertLiteralSymbol(analysis.uri(), "/blah.txt");
        assertThat(analysis.settings().get("compression"), is("gzip"));
    }

    @Test ( expected = UnsupportedFeatureException.class )
    public void testCopyToFileWithPartitionedTable() throws Exception {
        analyze("copy parted to '/blah.txt'");
    }
}

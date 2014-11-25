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

import io.crate.PartitionName;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.testing.MockedClusterServiceModule;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.lucene.BytesRefs;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CopyAnalyzerTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
                new MockedClusterServiceModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new OperatorModule())
        );
        return modules;
    }

    @Test
    public void testCopyFromExistingTable() throws Exception {
        CopyAnalyzedStatement analysis = (CopyAnalyzedStatement)analyze("copy users from '/some/distant/file.ext'");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertLiteralSymbol(analysis.uri(), "/some/distant/file.ext");
    }

    @Test
    public void testCopyFromExistingPartitionedTable() throws Exception {
        CopyAnalyzedStatement analysis = (CopyAnalyzedStatement)analyze("copy parted from '/some/distant/file.ext'");
        assertThat(analysis.table().ident(), is(TEST_PARTITIONED_TABLE_IDENT));
        assertLiteralSymbol(analysis.uri(), "/some/distant/file.ext");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testCopyFromPartitionedTablePARTITIONKeywordTooManyArgs() throws Exception {
        analyze("copy parted partition (a=1, b=2, c=3) from '/some/distant/file.ext'");
    }

    @Test
    public void testCopyFromPartitionedTablePARTITIONKeywordValidArgs() throws Exception {
        CopyAnalyzedStatement analysis = (CopyAnalyzedStatement) analyze(
                "copy parted partition (date=1395874800000) from '/some/distant/file.ext'");
        String parted = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).encodeIdent();
        assertThat(analysis.partitionIdent(), equalTo(parted));
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
        CopyAnalyzedStatement analysis = (CopyAnalyzedStatement)analyze("copy users from ?", new Object[]{path});
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        Object value = ((Literal) analysis.uri()).value();
        assertThat(BytesRefs.toString(value), is(path));
    }

    @Test
    public void testCopyToFile() throws Exception {
        CopyAnalyzedStatement analysis = (CopyAnalyzedStatement)analyze("copy users to '/blah.txt'");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.mode(), is(CopyAnalyzedStatement.Mode.TO));
        assertLiteralSymbol(analysis.uri(), "/blah.txt");
    }

    @Test
    public void testCopyToDirectory() throws Exception {
        CopyAnalyzedStatement analysis = (CopyAnalyzedStatement)analyze("copy users to directory '/foo'");
        assertThat(analysis.directoryUri(), is(true));
    }

    @Test
    public void testCopyToWithColumnList() throws Exception {
        CopyAnalyzedStatement analysis = (CopyAnalyzedStatement)analyze("copy users (id, name) to DIRECTORY '/tmp'");
        assertThat(analysis.outputSymbols().size(), is(2));
        assertThat(((Reference)analysis.outputSymbols().get(0)).info().ident().columnIdent().name(), is("id"));
        assertThat(((Reference)analysis.outputSymbols().get(1)).info().ident().columnIdent().name(), is("name"));
    }

    @Test
    public void testCopyToFileWithParams() throws Exception {
        CopyAnalyzedStatement analysis = (CopyAnalyzedStatement)analyze("copy users to '/blah.txt' with (compression='gzip')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.mode(), is(CopyAnalyzedStatement.Mode.TO));

        assertLiteralSymbol(analysis.uri(), "/blah.txt");
        assertThat(analysis.settings().get("compression"), is("gzip"));
    }

    @Test
    public void testCopyToFileWithPartitionedTable() throws Exception {
        CopyAnalyzedStatement analysis = (CopyAnalyzedStatement) analyze("copy parted to '/blah.txt'");
        assertThat(analysis.table().ident(), is(TEST_PARTITIONED_TABLE_IDENT));
        assertThat(analysis.mode(), is(CopyAnalyzedStatement.Mode.TO));
    }

    @Test
    public void testCopyToFileWithPartitionClause() throws Exception {
        CopyAnalyzedStatement analysis = (CopyAnalyzedStatement) analyze("copy parted partition (date=1395874800000) to '/blah.txt'");
        String parted = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).encodeIdent();
        assertThat(analysis.partitionIdent(), is(parted));
    }

    @Test
    public void testCopyToDirectoryithPartitionClause() throws Exception {
        CopyAnalyzedStatement analysis = (CopyAnalyzedStatement) analyze("copy parted partition (date=1395874800000) to directory '/tmp'");
        assertThat(analysis.directoryUri(), is(true));
        String parted = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).encodeIdent();
        assertThat(analysis.partitionIdent(), is(parted));
    }


    @Test
    public void testCopyToDirectoryWithNotExistingPartitionClause() throws Exception {
        expectedException.expect(PartitionUnknownException.class);
        expectedException.expectMessage("No partition for table 'parted' with ident '04130' exists");
        analyze("copy parted partition (date=0) to directory '/tmp/'");
    }

    @Test
    public void testCopyToWithNotExistingPartitionClause() throws Exception {
        expectedException.expect(PartitionUnknownException.class);
        expectedException.expectMessage("No partition for table 'parted' with ident '04130' exists");
        analyze("copy parted partition (date=0) to '/tmp/blah.txt'");
    }




    @Test
    public void testCopyFromWithReferenceAssignedToProperty() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Can't use column reference in property assignment \"compression = gzip\". Use literals instead.");
        analyze("copy users from '/blah.txt' with (compression = gzip)");
    }
}

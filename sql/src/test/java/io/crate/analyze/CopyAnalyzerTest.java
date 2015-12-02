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

import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.planner.projection.WriterProjection;
import io.crate.testing.MockedClusterServiceModule;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.lucene.BytesRefs;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.isFunction;
import static io.crate.testing.TestingHelpers.isLiteral;
import static io.crate.testing.TestingHelpers.isReference;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
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
            when(schemaInfo.name()).thenReturn(Schemas.DEFAULT_SCHEMA_NAME);
            when(schemaInfo.getTableInfo(USER_TABLE_IDENT.name())).thenReturn(USER_TABLE_INFO);
            when(schemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name())).thenReturn(TEST_PARTITIONED_TABLE_INFO);
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
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
        CopyFromAnalyzedStatement analysis = (CopyFromAnalyzedStatement)analyze("copy users from '/some/distant/file.ext'");
        assertThat(analysis.table().ident(), is(USER_TABLE_IDENT));
        assertThat(analysis.uri(), isLiteral("/some/distant/file.ext"));
    }

    @Test
    public void testCopyFromExistingPartitionedTable() throws Exception {
        CopyFromAnalyzedStatement analysis = (CopyFromAnalyzedStatement)analyze("copy parted from '/some/distant/file.ext'");
        assertThat(analysis.table().ident(), is(TEST_PARTITIONED_TABLE_IDENT));
        assertThat(analysis.uri(), isLiteral("/some/distant/file.ext"));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testCopyFromPartitionedTablePARTITIONKeywordTooManyArgs() throws Exception {
        analyze("copy parted partition (a=1, b=2, c=3) from '/some/distant/file.ext'");
    }

    @Test
    public void testCopyFromPartitionedTablePARTITIONKeywordValidArgs() throws Exception {
        CopyFromAnalyzedStatement analysis = (CopyFromAnalyzedStatement) analyze(
                "copy parted partition (date=1395874800000) from '/some/distant/file.ext'");
        String parted = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).ident();
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
        CopyFromAnalyzedStatement analysis = (CopyFromAnalyzedStatement)analyze("copy users from ?", new Object[]{path});
        assertThat(analysis.table().ident(), is(USER_TABLE_IDENT));
        Object value = ((Literal) analysis.uri()).value();
        assertThat(BytesRefs.toString(value), is(path));
    }

    @Test
    public void testCopyToFile() throws Exception {
        CopyToAnalyzedStatement analysis = (CopyToAnalyzedStatement)analyze("copy users to '/blah.txt'");
        TableInfo tableInfo = ((QueriedDocTable) analysis.subQueryRelation()).tableRelation().tableInfo();
        assertThat(tableInfo.ident(), is(USER_TABLE_IDENT));
        assertThat(analysis.uri(), isLiteral("/blah.txt"));
    }

    @Test
    public void testCopyToDirectory() throws Exception {
        CopyToAnalyzedStatement analysis = (CopyToAnalyzedStatement)analyze("copy users to directory '/foo'");
        assertThat(analysis.isDirectoryUri(), is(true));
    }

    @Test
    public void testCopySysTableTo() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot COPY sys.nodes TO. COPY TO only supports user tables");
        analyze("copy sys.nodes to directory '/foo'");
    }

    @Test
    public void testCopyToWithColumnList() throws Exception {
        CopyToAnalyzedStatement analysis = (CopyToAnalyzedStatement)analyze("copy users (id, name) to DIRECTORY '/tmp'");
        assertThat(analysis.subQueryRelation(), instanceOf(QueriedDocTable.class));
        QuerySpec querySpec = ((QueriedDocTable) analysis.subQueryRelation()).querySpec();
        assertThat(querySpec.outputs().size(), is(2));
        assertThat(querySpec.outputs().get(0), isReference("_doc['id']"));
        assertThat(querySpec.outputs().get(1), isReference("_doc['name']"));
    }

    @Test
    public void testCopyToFileWithCompressionParams() throws Exception {
        CopyToAnalyzedStatement analysis = (CopyToAnalyzedStatement)analyze("copy users to '/blah.txt' with (compression='gzip')");
        TableInfo tableInfo = ((QueriedDocTable) analysis.subQueryRelation()).tableRelation().tableInfo();
        assertThat(tableInfo.ident(), is(USER_TABLE_IDENT));

        assertThat(analysis.uri(), isLiteral("/blah.txt"));
        assertThat(analysis.compressionType(), is(WriterProjection.CompressionType.GZIP));
    }

    @Test
    public void testCopyToFileWithUnknownParams() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Unknown setting 'foo'");
        CopyToAnalyzedStatement analysis = (CopyToAnalyzedStatement)analyze("copy users to '/blah.txt' with (foo='gzip')");
    }

    @Test
    public void testCopyToFileWithPartitionedTable() throws Exception {
        CopyToAnalyzedStatement analysis = (CopyToAnalyzedStatement) analyze("copy parted to '/blah.txt'");
        TableInfo tableInfo = ((QueriedDocTable) analysis.subQueryRelation()).tableRelation().tableInfo();
        assertThat(tableInfo.ident(), is(TEST_PARTITIONED_TABLE_IDENT));
        assertThat(analysis.overwrites().size(), is(1));
    }

    @Test
    public void testCopyToFileWithPartitionClause() throws Exception {
        CopyToAnalyzedStatement analysis = (CopyToAnalyzedStatement) analyze("copy parted partition (date=1395874800000) to '/blah.txt'");
        String parted = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName();
        QuerySpec querySpec = ((QueriedDocTable) analysis.subQueryRelation()).querySpec();
        assertThat(querySpec.where().partitions(), contains(parted));
    }

    @Test
    public void testCopyToDirectoryWithPartitionClause() throws Exception {
        CopyToAnalyzedStatement analysis = (CopyToAnalyzedStatement) analyze("copy parted partition (date=1395874800000) to directory '/tmp'");
        assertThat(analysis.isDirectoryUri(), is(true));
        String parted = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName();
        QuerySpec querySpec = ((QueriedDocTable) analysis.subQueryRelation()).querySpec();
        assertThat(querySpec.where().partitions(), contains(parted));
        assertThat(analysis.overwrites().size(), is(0));
    }

    @Test
    public void testCopyToDirectoryWithNotExistingPartitionClause() throws Exception {
        expectedException.expect(PartitionUnknownException.class);
        expectedException.expectMessage("No partition for table 'doc.parted' with ident '04130' exists");
        analyze("copy parted partition (date=0) to directory '/tmp/'");
    }

    @Test
    public void testCopyToWithWhereClause() throws Exception {
        CopyToAnalyzedStatement analysis = (CopyToAnalyzedStatement) analyze("copy parted where id = 1 to '/tmp/foo.json'");
        QuerySpec querySpec = ((QueriedDocTable) analysis.subQueryRelation()).querySpec();
        assertThat(querySpec.where().query(), isFunction("op_="));
    }

    @Test
    public void testCopyToWithPartitionIdentAndPartitionInWhereClause() throws Exception {
        CopyToAnalyzedStatement analysis = (CopyToAnalyzedStatement) analyze(
                "copy parted partition (date=1395874800000) where date = 1395874800000 to '/tmp/foo.json'");
        String parted = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName();
        QuerySpec querySpec = ((QueriedDocTable) analysis.subQueryRelation()).querySpec();
        assertThat(querySpec.where().partitions(), contains(parted));
    }

    @Test
    public void testCopyToWithPartitionInWhereClause() throws Exception {
        CopyToAnalyzedStatement analysis = (CopyToAnalyzedStatement) analyze(
                "copy parted where date = 1395874800000 to '/tmp/foo.json'");
        String parted = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName();
        QuerySpec querySpec = ((QueriedDocTable) analysis.subQueryRelation()).querySpec();
        assertThat(querySpec.where().partitions(), contains(parted));
        assertThat(analysis.overwrites().size(), is(1));
    }

    @Test
    public void testCopyToWithPartitionIdentAndWhereClause() throws Exception {
        CopyToAnalyzedStatement analysis = (CopyToAnalyzedStatement) analyze(
                "copy parted partition (date=1395874800000) where id = 1 to '/tmp/foo.json'");
        String parted = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName();
        QuerySpec querySpec = ((QueriedDocTable) analysis.subQueryRelation()).querySpec();
        assertThat(querySpec.where().partitions(), contains(parted));
        assertThat(querySpec.where().query(), isFunction("op_="));
    }

    @Test
    public void testCopyToWithInvalidPartitionInWhereClause() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Given partition ident does not match partition evaluated from where clause");
        analyze("copy parted partition (date=1395874800000) where date = 1395961200000 to '/tmp/foo.json'");
    }

    @Test
    public void testCopyToWithNotExistingPartitionClause() throws Exception {
        expectedException.expect(PartitionUnknownException.class);
        expectedException.expectMessage("No partition for table 'doc.parted' with ident '04130' exists");
        analyze("copy parted partition (date=0) to '/tmp/blah.txt'");
    }

    @Test
    public void testCopyFromWithReferenceAssignedToProperty() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Can't use column reference in property assignment \"compression = gzip\". Use literals instead.");
        analyze("copy users from '/blah.txt' with (compression = gzip)");
    }
}

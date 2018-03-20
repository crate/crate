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

import com.google.common.collect.ImmutableList;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import static io.crate.analyze.TableDefinitions.SHARD_ROUTING;
import static io.crate.analyze.TableDefinitions.USER_TABLE_IDENT;
import static java.util.Locale.ENGLISH;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DropTableAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private final TableIdent aliasIdent = new TableIdent(DocSchemaInfo.NAME, "alias_table");
    private final DocTableInfo aliasInfo = TestingTableInfo.builder(aliasIdent, SHARD_ROUTING)
        .add("col", DataTypes.STRING, ImmutableList.<String>of())
        .isAlias(true)
        .build();

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().addDocTable(aliasInfo).build();
    }

    @Test
    public void testDropNonExistingTable() throws Exception {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'doc.unknown' unknown");
        e.analyze("drop table unknown");
    }

    @Test
    public void testDropSystemTable() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"sys.cluster\" doesn't support or allow DROP " +
                                        "operations, as it is read-only.");
        e.analyze("drop table sys.cluster");
    }

    @Test
    public void testDropInformationSchemaTable() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"information_schema.tables\" doesn't support or allow " +
                                        "DROP operations, as it is read-only.");
        e.analyze("drop table information_schema.tables");
    }

    @Test
    public void testDropUnknownSchema() throws Exception {
        expectedException.expect(SchemaUnknownException.class);
        expectedException.expectMessage("Schema 'unknown_schema' unknown");
        e.analyze("drop table unknown_schema.unknown");
    }

    @Test
    public void testDropTableIfExistsWithUnknownSchema() throws Exception {
        // shouldn't raise SchemaUnknownException / RelationUnknown
        e.analyze("drop table if exists unknown_schema.unknown");
    }

    @Test
    public void testDropExistingTable() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze(String.format(ENGLISH, "drop table %s", USER_TABLE_IDENT.name()));
        assertThat(analyzedStatement, instanceOf(DropTableAnalyzedStatement.class));
        DropTableAnalyzedStatement dropTableAnalysis = (DropTableAnalyzedStatement) analyzedStatement;
        assertThat(dropTableAnalysis.dropIfExists(), is(false));
        assertThat(dropTableAnalysis.index(), is(USER_TABLE_IDENT.name()));
    }

    @Test
    public void testDropIfExistExistingTable() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze(String.format(ENGLISH, "drop table if exists %s", USER_TABLE_IDENT.name()));
        assertThat(analyzedStatement, instanceOf(DropTableAnalyzedStatement.class));
        DropTableAnalyzedStatement dropTableAnalysis = (DropTableAnalyzedStatement) analyzedStatement;
        assertThat(dropTableAnalysis.dropIfExists(), is(true));
        assertThat(dropTableAnalysis.index(), is(USER_TABLE_IDENT.name()));
    }

    @Test
    public void testNonExistentTableIsRecognizedCorrectly() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("drop table if exists unknowntable");
        assertThat(analyzedStatement, instanceOf(DropTableAnalyzedStatement.class));
        DropTableAnalyzedStatement dropTableAnalysis = (DropTableAnalyzedStatement) analyzedStatement;
        assertThat(dropTableAnalysis.dropIfExists(), is(true));
        assertThat(dropTableAnalysis.noop(), is(true));
    }

    @Test
    public void testDropAliasFails() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"doc.alias_table\" doesn't support or allow DROP " +
                                        "operations, as it is read-only.");
        e.analyze("drop table alias_table");
    }

    @Test
    public void testDropAliasIfExists() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"doc.alias_table\" doesn't support or allow DROP " +
                                        "operations, as it is read-only.");
        e.analyze("drop table if exists alias_table");
    }
}

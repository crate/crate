/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import static io.crate.analyze.TableDefinitions.USER_TABLE_IDENT;
import static java.util.Locale.ENGLISH;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.scoped.table.OperationOnInaccessibleRelationException;
import io.crate.exceptions.scoped.table.RelationUnknown;
import io.crate.exceptions.scoped.schema.SchemaUnknownException;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class DropTableAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
    }

    @Test
    public void testDropNonExistingTable() throws Exception {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'unknown' unknown");
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
        AnalyzedDropTable<DocTableInfo> dropTable = e.analyze(String.format(ENGLISH, "drop table %s", USER_TABLE_IDENT.name()));
        assertThat(dropTable.dropIfExists(), is(false));
        assertThat(dropTable.table().ident().indexNameOrAlias(), is(USER_TABLE_IDENT.name()));
    }

    @Test
    public void testDropIfExistExistingTable() throws Exception {
        AnalyzedDropTable<DocTableInfo> dropTable = e.analyze(String.format(ENGLISH, "drop table if exists %s", USER_TABLE_IDENT.name()));
        assertThat(dropTable.dropIfExists(), is(true));
        assertThat(dropTable.table().ident().indexNameOrAlias(), is(USER_TABLE_IDENT.name()));
    }

    @Test
    public void testNonExistentTableIsRecognizedCorrectly() throws Exception {
        AnalyzedDropTable<TableInfo> dropTable = e.analyze("drop table if exists unknowntable");
        assertThat(dropTable.table(), Matchers.nullValue());
    }
}

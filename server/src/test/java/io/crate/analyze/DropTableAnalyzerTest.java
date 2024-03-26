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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class DropTableAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION);
    }

    @Test
    public void testDropNonExistingTable() {
        assertThatThrownBy(() -> e.analyze("drop table unknown"))
            .isExactlyInstanceOf(RelationUnknown.class)
            .hasMessage("Relation 'unknown' unknown");
        assertThatThrownBy(() -> e.analyze("drop table doc.unknown"))
            .isExactlyInstanceOf(RelationUnknown.class)
            .hasMessage("Relation 'doc.unknown' unknown");
        assertThatThrownBy(() -> e.analyze("drop table crate.doc.unknown"))
            .isExactlyInstanceOf(RelationUnknown.class)
            .hasMessage("Relation 'doc.unknown' unknown");
    }

    @Test
    public void testDropSystemTable() {
        assertThatThrownBy(() -> e.analyze("drop table sys.cluster"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"sys.cluster\" doesn't support or allow DROP " +
                        "operations");
    }

    @Test
    public void testDropInformationSchemaTable() {
        assertThatThrownBy(() -> e.analyze("drop table information_schema.tables"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"information_schema.tables\" doesn't support or allow " +
                        "DROP operations");
    }

    @Test
    public void testDropUnknownSchema() {
        assertThatThrownBy(() -> e.analyze("drop table unknown_schema.unknown"))
            .isExactlyInstanceOf(SchemaUnknownException.class)
            .hasMessage("Schema 'unknown_schema' unknown");
        assertThatThrownBy(() -> e.analyze("drop table crate.unknown_schema.unknown"))
            .isExactlyInstanceOf(SchemaUnknownException.class)
            .hasMessage("Schema 'unknown_schema' unknown");
    }

    @Test
    public void testDropTableIfExistsWithUnknownSchema() {
        // shouldn't raise SchemaUnknownException / RelationUnknown
        e.analyze("drop table if exists unknown_schema.unknown");
        e.analyze("drop table if exists crate.unknown_schema.unknown");
    }

    @Test
    public void testDropExistingTable() {
        AnalyzedDropTable<DocTableInfo> dropTable = e.analyze(String.format(ENGLISH, "drop table %s", USER_TABLE_IDENT.name()));
        assertThat(dropTable.dropIfExists()).isFalse();
        assertThat(dropTable.table().ident().indexNameOrAlias()).isEqualTo(USER_TABLE_IDENT.name());
    }

    @Test
    public void testDropIfExistExistingTable() {
        AnalyzedDropTable<DocTableInfo> dropTable = e.analyze(String.format(ENGLISH, "drop table if exists %s", USER_TABLE_IDENT.name()));
        assertThat(dropTable.dropIfExists()).isTrue();
        assertThat(dropTable.table().ident().indexNameOrAlias()).isEqualTo(USER_TABLE_IDENT.name());
    }

    @Test
    public void testNonExistentTableIsRecognizedCorrectly() {
        AnalyzedDropTable<TableInfo> dropTable = e.analyze("drop table if exists unknowntable");
        assertThat(dropTable.table()).isNull();
    }
}

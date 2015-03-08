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
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.ClusterService;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DropTableAnalyzedStatementTest extends CrateUnitTest {

    public static final String IRRELEVANT = "Irrelevant";

    private ReferenceInfos referenceInfos;

    private SchemaInfo docSchemaInfo;

    private DropTableAnalyzedStatement dropTableAnalyzedStatement;

    @Before
    public void prepare() {
        referenceInfos = mock(ReferenceInfos.class);
        docSchemaInfo = mock(DocSchemaInfo.class);
        when(docSchemaInfo.getTableInfo(any(String.class))).thenReturn(null);
    }

    @Test
    public void unknownTableRaisesExceptionIfNotIgnored() throws Exception {
        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage(format("Table '%s.%s' unknown", IRRELEVANT, IRRELEVANT));


        getSchemaInfoReturns(docSchemaInfo);

        TableIdent tableIdent = new TableIdent(IRRELEVANT, IRRELEVANT);

        dropTableAnalyzedStatement = new DropTableAnalyzedStatement(referenceInfos, false);

        dropTableAnalyzedStatement.table(tableIdent);

    }

    @Test
    public void unknownTableSetsNoopIfIgnoreNonExistentTablesIsSet() throws Exception {

        getSchemaInfoReturns(docSchemaInfo);

        TableIdent tableIdent = new TableIdent(IRRELEVANT, IRRELEVANT);

        dropTableAnalyzedStatement = new DropTableAnalyzedStatement(referenceInfos, true);

        dropTableAnalyzedStatement.table(tableIdent);
        assertThat(dropTableAnalyzedStatement.noop(), is(true));

    }

    @Test
    public void knownTableDoesNotSetNoop() {
        TableInfo tableInfo = mock(DocTableInfo.class);
        when(tableInfo.isPartitioned()).thenReturn(false);
        when(tableInfo.isAlias()).thenReturn(false);
        when(docSchemaInfo.getTableInfo(any(String.class))).thenReturn(tableInfo);

        TableIdent tableIdent = new TableIdent(IRRELEVANT, IRRELEVANT);
        getSchemaInfoReturns(docSchemaInfo);

        dropTableAnalyzedStatement = new DropTableAnalyzedStatement(referenceInfos, true);

        dropTableAnalyzedStatement.table(tableIdent);
        assertThat(dropTableAnalyzedStatement.noop(), is(false));
    }

    @Test
    public void deletingAliasRaisesException() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Table alias not allowed in DROP TABLE statement.");

        TableInfo tableInfo = mock(DocTableInfo.class);
        when(tableInfo.isPartitioned()).thenReturn(false);
        when(tableInfo.isAlias()).thenReturn(true);
        when(docSchemaInfo.getTableInfo(any(String.class))).thenReturn(tableInfo);

        TableIdent tableIdent = new TableIdent(IRRELEVANT, IRRELEVANT);
        getSchemaInfoReturns(docSchemaInfo);

        dropTableAnalyzedStatement = new DropTableAnalyzedStatement(referenceInfos, true);

        dropTableAnalyzedStatement.table(tableIdent);
    }

    @Test
    public void deletingSystemSchemaRaisesException() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(format("cannot delete 'System.%s'", IRRELEVANT));

        getSchemaInfoReturns(new SysSchemaInfo(mock(ClusterService.class)));
        TableIdent ident = new TableIdent("System", IRRELEVANT);

        dropTableAnalyzedStatement = new DropTableAnalyzedStatement(referenceInfos, false);

        dropTableAnalyzedStatement.table(ident);
    }

    @Test
    public void deletingNullSchemaRaisesException() throws Exception {
        expectedException.expect(SchemaUnknownException.class);
        expectedException.expectMessage("Schema 'null' unknown");

        getSchemaInfoReturns(null);
        TableIdent ident = new TableIdent();

        dropTableAnalyzedStatement = new DropTableAnalyzedStatement(referenceInfos, false);

        dropTableAnalyzedStatement.table(ident);
    }

    private void getSchemaInfoReturns(SchemaInfo schemaInfo) {
        when(referenceInfos.getSchemaInfo(anyString())).thenReturn(schemaInfo);
    }
}
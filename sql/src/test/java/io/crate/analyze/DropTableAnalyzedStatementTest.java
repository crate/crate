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


import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Locale;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DropTableAnalyzedStatementTest extends CrateUnitTest {

    public static final String IRRELEVANT = "Irrelevant";

    private Schemas schemas;

    private DropTableAnalyzedStatement dropTableAnalyzedStatement;

    @Before
    public void prepare() {
        schemas = mock(Schemas.class);
    }

    @Test
    public void testUnknownTableRaisesExceptionIfNotIgnored() throws Exception {
        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "Table '%s.%s' unknown", IRRELEVANT, IRRELEVANT));

        TableIdent tableIdent = new TableIdent(IRRELEVANT, IRRELEVANT);
        when(schemas.getDropableTable(tableIdent)).thenThrow(new TableUnknownException(tableIdent));

        dropTableAnalyzedStatement = new DropTableAnalyzedStatement(schemas, false);
        dropTableAnalyzedStatement.table(tableIdent);
    }

    @Test
    public void unknownTableSetsNoopIfIgnoreNonExistentTablesIsSet() throws Exception {
        TableIdent tableIdent = new TableIdent(IRRELEVANT, IRRELEVANT);
        when(schemas.getDropableTable(tableIdent)).thenThrow(new TableUnknownException(tableIdent));

        dropTableAnalyzedStatement = new DropTableAnalyzedStatement(schemas, true);
        dropTableAnalyzedStatement.table(tableIdent);

        assertThat(dropTableAnalyzedStatement.noop(), is(true));
    }

    @Test
    public void knownTableDoesNotSetNoop() {
        TableIdent tableIdent = new TableIdent(IRRELEVANT, IRRELEVANT);
        DocTableInfo tableInfo = mock(DocTableInfo.class);
        when(tableInfo.isPartitioned()).thenReturn(false);
        when(tableInfo.isAlias()).thenReturn(false);

        when(schemas.getDropableTable(tableIdent)).thenReturn(tableInfo);
        dropTableAnalyzedStatement = new DropTableAnalyzedStatement(schemas, true);

        dropTableAnalyzedStatement.table(tableIdent);
        assertThat(dropTableAnalyzedStatement.noop(), is(false));
    }
}

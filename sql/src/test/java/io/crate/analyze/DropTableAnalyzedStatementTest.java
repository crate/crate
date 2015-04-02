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
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DropTableAnalyzedStatementTest extends CrateUnitTest {

    public static final String IRRELEVANT = "Irrelevant";

    private ReferenceInfos referenceInfos;

    private DropTableAnalyzedStatement dropTableAnalyzedStatement;

    @Before
    public void prepare() {
        referenceInfos = mock(ReferenceInfos.class);
    }

    @Test
    public void testUnknownTableRaisesExceptionIfNotIgnored() throws Exception {
        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage(String.format("Table '%s.%s' unknown", IRRELEVANT, IRRELEVANT));

        TableIdent tableIdent = new TableIdent(IRRELEVANT, IRRELEVANT);
        when(referenceInfos.getWritableTable(tableIdent)).thenThrow(new TableUnknownException(tableIdent));

        dropTableAnalyzedStatement = new DropTableAnalyzedStatement(referenceInfos, false);
        dropTableAnalyzedStatement.table(tableIdent);
    }

    @Test
    public void unknownTableSetsNoopIfIgnoreNonExistentTablesIsSet() throws Exception {
        TableIdent tableIdent = new TableIdent(IRRELEVANT, IRRELEVANT);
        when(referenceInfos.getWritableTable(tableIdent)).thenThrow(new TableUnknownException(tableIdent));

        dropTableAnalyzedStatement = new DropTableAnalyzedStatement(referenceInfos, true);
        dropTableAnalyzedStatement.table(tableIdent);

        assertThat(dropTableAnalyzedStatement.noop(), is(true));
    }

    @Test
    public void knownTableDoesNotSetNoop() {
        TableInfo tableInfo = mock(DocTableInfo.class);
        when(tableInfo.isPartitioned()).thenReturn(false);
        when(tableInfo.isAlias()).thenReturn(false);

        TableIdent tableIdent = new TableIdent(IRRELEVANT, IRRELEVANT);

        dropTableAnalyzedStatement = new DropTableAnalyzedStatement(referenceInfos, true);

        dropTableAnalyzedStatement.table(tableIdent);
        assertThat(dropTableAnalyzedStatement.noop(), is(false));
    }
}
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
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Before;
import org.junit.Test;

import static io.crate.metadata.blob.BlobSchemaInfo.NAME;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DropBlobTableAnalyzedStatementTest extends CrateUnitTest {

    public static final String IRRELEVANT = "Irrelevant";

    private ReferenceInfos referenceInfos;

    private DropBlobTableAnalyzedStatement dropBlobTableAnalyzedStatement;

    private SchemaInfo schemaInfo;

    @Before
    public void prepare() {
        referenceInfos = mock(ReferenceInfos.class);

        schemaInfo = mock(BlobSchemaInfo.class);
        when(referenceInfos.getSchemaInfo(any(String.class))).thenReturn(schemaInfo);
    }

    @Test
    public void deletingNoExistingTableSetsNoopIfIgnoreNonExistentTablesIsSet() throws Exception {
        TableIdent tableIdent = new TableIdent(NAME, IRRELEVANT);

        dropBlobTableAnalyzedStatement = new DropBlobTableAnalyzedStatement(referenceInfos, true);

        dropBlobTableAnalyzedStatement.table(tableIdent);

        assertThat(dropBlobTableAnalyzedStatement.noop(), is(true));
    }

    @Test
    public void deletingAliasTableRaisesException() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Table alias not allowed in DROP BLOB TABLE statement.");

        TableInfo tableInfo = mock(BlobTableInfo.class);
        when(tableInfo.isPartitioned()).thenReturn(false);
        when(tableInfo.isAlias()).thenReturn(true);
        when(schemaInfo.getTableInfo(any(String.class))).thenReturn(tableInfo);

        TableIdent tableIdent = new TableIdent(NAME, IRRELEVANT);

        dropBlobTableAnalyzedStatement = new DropBlobTableAnalyzedStatement(referenceInfos, false);

        dropBlobTableAnalyzedStatement.table(tableIdent);
    }

    @Test
    public void deletingNonExistingTableRaisesException() throws Exception {

        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage("Table 'Irrelevant' unknown");

        TableIdent tableIdent = new TableIdent(NAME, IRRELEVANT);

        dropBlobTableAnalyzedStatement = new DropBlobTableAnalyzedStatement(referenceInfos, false);

        dropBlobTableAnalyzedStatement.table(tableIdent);
    }


}
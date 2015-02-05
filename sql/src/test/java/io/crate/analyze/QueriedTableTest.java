/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.isField;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class QueriedTableTest {

    @Test
    public void testNewSubRelation() throws Exception {
        TableRelation tr1 = new TableRelation(mock(TableInfo.class));
        TableRelation tr2 = new TableRelation(mock(TableInfo.class));

        QuerySpec querySpec = new QuerySpec();
        querySpec.outputs(Arrays.<Symbol>asList(
                new Field(tr1, new ColumnIdent("x"), DataTypes.STRING),
                new Field(tr2, new ColumnIdent("y"), DataTypes.STRING)
        ));
        QueriedTable queriedTable = QueriedTable.newSubRelation(new QualifiedName("t"), tr1, querySpec);
        assertThat(queriedTable.querySpec().outputs().size(), is(1));
        assertThat(queriedTable.querySpec().outputs().get(0), isField("x"));

         // output of original querySpec has been rewritten, fields now point to the QueriedTable
        assertTrue(((Field) querySpec.outputs().get(0)).relation() == queriedTable);
        assertThat(querySpec.outputs().get(0), isField("x"));
    }
}
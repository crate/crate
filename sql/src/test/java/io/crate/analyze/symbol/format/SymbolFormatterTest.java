/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.symbol.format;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.IllegalFormatConversionException;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class SymbolFormatterTest extends CrateUnitTest {

    SqlExpressions sqlExpressions;
    SymbolPrinter printer;

    public static final String TABLE_NAME = "formatter";

    @Before
    public void prepare() throws Exception {
        DocTableInfo tableInfo = TestingTableInfo.builder(new TableIdent(DocSchemaInfo.NAME, TABLE_NAME), null)
                .add("foo", DataTypes.STRING)
                .add("bar", DataTypes.LONG)
                .add("CraZy", DataTypes.IP)
                .add("select", DataTypes.BYTE)
                .build();
        Map<QualifiedName, AnalyzedRelation> sources = ImmutableMap.<QualifiedName, AnalyzedRelation>builder()
                .put(QualifiedName.of(TABLE_NAME), new TableRelation(tableInfo))
                .build();
        sqlExpressions = new SqlExpressions(sources);
        printer = new SymbolPrinter(sqlExpressions.analysisMD().functions());
    }

    @Test
    public void testFormat() throws Exception {
        Function f = new Function(new FunctionInfo(
                new FunctionIdent("foo", Arrays.<DataType>asList(DataTypes.STRING, DataTypes.UNDEFINED)), DataTypes.DOUBLE),
                Arrays.<Symbol>asList(Literal.newLiteral("bar"), Literal.newLiteral(3.4)));
        assertThat(SymbolFormatter.format("This Symbol is formatted %s", f), is("This Symbol is formatted foo('bar', 3.4)"));
    }

    @Test
    public void testFormatInvalidEscape() throws Exception {
        expectedException.expect(IllegalFormatConversionException.class);
        expectedException.expectMessage("d != java.lang.String");
        assertThat(SymbolFormatter.format("%d", Literal.newLiteral(42L)), is(""));
    }
}

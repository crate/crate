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

package io.crate.metadata;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.collect.MapBuilder;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;


public class ReferenceToLiteralConverterTest extends CrateUnitTest {

    private static final TableIdent TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "characters");

    @Test
    public void testReplaceSimpleReference() throws Exception {
        Object[] inputValues = new Object[]{1};
        Reference idRef = new Reference(
            new ReferenceIdent(TABLE_IDENT, new ColumnIdent("id")), RowGranularity.DOC, DataTypes.INTEGER);

        ReferenceToLiteralConverter convertFunction = new ReferenceToLiteralConverter(
            ImmutableList.of(idRef), ImmutableList.of(idRef));
        convertFunction.values(inputValues);

        Symbol replacedSymbol = convertFunction.apply(idRef);
        assertThat(replacedSymbol, isLiteral(1, DataTypes.INTEGER));
    }

    @Test
    public void testReplaceSubscriptReference() throws Exception {
        Object[] inputValues = new Object[]{
            MapBuilder.newMapBuilder().put("name", "Ford").map()};

        Reference userRef = new Reference(
            new ReferenceIdent(TABLE_IDENT, new ColumnIdent("user")), RowGranularity.DOC, DataTypes.OBJECT);
        Reference nameRef = new Reference(
            new ReferenceIdent(TABLE_IDENT, new ColumnIdent("user", ImmutableList.of("name"))),
            RowGranularity.DOC, DataTypes.STRING);

        ReferenceToLiteralConverter convertFunction = new ReferenceToLiteralConverter(
            ImmutableList.of(userRef), ImmutableList.of(nameRef));
        convertFunction.values(inputValues);

        Symbol replacedSymbol = convertFunction.apply(nameRef);
        assertThat(replacedSymbol, isLiteral("Ford", DataTypes.STRING));
    }

    @Test
    public void testReplaceNestedSubscriptReference() throws Exception {
        Object[] inputValues = new Object[]{
            MapBuilder.newMapBuilder().put("profile",
                MapBuilder.newMapBuilder().put("name", "Ford").map()
            ).map()};

        Reference userRef = new Reference(
            new ReferenceIdent(TABLE_IDENT, new ColumnIdent("user")), RowGranularity.DOC, DataTypes.OBJECT);
        Reference nameRef = new Reference(
            new ReferenceIdent(TABLE_IDENT, new ColumnIdent("user", ImmutableList.of("profile", "name"))),
            RowGranularity.DOC, DataTypes.STRING);

        ReferenceToLiteralConverter convertFunction = new ReferenceToLiteralConverter(
            ImmutableList.of(userRef), ImmutableList.of(nameRef));
        convertFunction.values(inputValues);

        Symbol replacedSymbol = convertFunction.apply(nameRef);
        assertThat(replacedSymbol, isLiteral("Ford", DataTypes.STRING));
    }
}

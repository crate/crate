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
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.expressions.ValueNormalizer;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.InvalidColumnNameException;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.omg.CORBA.DynAnyPackage.Invalid;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ValueNormalizerTest extends CrateUnitTest {

    private static final TableIdent TEST_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "test1");
    private static final TableInfo userTableInfo = TestingTableInfo.builder(TEST_TABLE_IDENT,
        new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
        .add("id", DataTypes.LONG, null)
        .add("name", DataTypes.STRING, null)
        .add("d", DataTypes.DOUBLE, null)
        .add("dyn_empty", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        .add("dyn", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        .add("dyn", DataTypes.DOUBLE, ImmutableList.of("d"))
        .add("dyn", DataTypes.OBJECT, ImmutableList.of("inner_strict"), ColumnPolicy.STRICT)
        .add("dyn", DataTypes.DOUBLE, ImmutableList.of("inner_strict", "double"))
        .add("strict", DataTypes.OBJECT, null, ColumnPolicy.STRICT)
        .add("strict", DataTypes.DOUBLE, ImmutableList.of("inner_d"))
        .add("ignored", DataTypes.OBJECT, null, ColumnPolicy.IGNORED)
        .addPrimaryKey("id")
        .clusteredBy("id")
        .build();

    private ValueNormalizer valueNormalizer;

    private Symbol normalizeInputForReference(Symbol valueSymbol, Reference reference) {
        return valueNormalizer.normalizeInputForReference(valueSymbol, reference, userTableInfo);
    }

    @Before
    public void prepare() {
        Schemas schemas = mock(Schemas.class);
        when(schemas.getTableInfo(TEST_TABLE_IDENT)).thenReturn(userTableInfo);
        valueNormalizer = new ValueNormalizer();
    }

    @Test
    public void testNormalizePrimitiveLiteral() throws Exception {
        Reference ref = new Reference(
            new ReferenceIdent(TEST_TABLE_IDENT, new ColumnIdent("bool")),
            RowGranularity.DOC,
            DataTypes.BOOLEAN
        );
        Literal<Boolean> trueLiteral = Literal.of(true);

        assertThat(normalizeInputForReference(trueLiteral, ref), Matchers.<Symbol>is(trueLiteral));
        assertThat(normalizeInputForReference(Literal.of("true"), ref), Matchers.<Symbol>is(trueLiteral));
        assertThat(normalizeInputForReference(Literal.of("false"), ref), Matchers.<Symbol>is(Literal.of(false)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeDynamicEmptyObjectLiteral() throws Exception {
        Reference objRef = userTableInfo.getReference(new ColumnIdent("dyn_empty"));
        Map<String, Object> map = new HashMap<>();
        map.put("time", "2014-02-16T00:00:01");
        map.put("false", true);
        Literal<Map<String, Object>> normalized = (Literal) normalizeInputForReference(
            Literal.of(map), objRef);
        assertThat((String) normalized.value().get("time"), is("2014-02-16T00:00:01"));
        assertThat((Boolean) normalized.value().get("false"), is(true));
    }

    @Test(expected = ColumnValidationException.class)
    public void testNormalizeObjectLiteralInvalidNested() throws Exception {
        Reference objRef = userTableInfo.getReference(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("d", "2014-02-16T00:00:01");
        normalizeInputForReference(Literal.of(map), objRef);
    }

    @Test
    public void testNormalizeObjectLiteralConvertFromString() throws Exception {
        Reference objInfo = userTableInfo.getReference(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("d", "2.9");

        Symbol normalized = normalizeInputForReference(Literal.of(map), objInfo);
        assertThat(normalized, instanceOf(Literal.class));
        assertThat(((Literal<Map<String, Object>>) normalized).value().get("d"), Matchers.<Object>is(2.9d));
    }

    @Test
    public void testNormalizeObjectLiteral() throws Exception {
        Reference objInfo = userTableInfo.getReference(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<String, Object>() {{
            put("d", 2.9d);
            put("inner_strict", new HashMap<String, Object>() {{
                put("double", "-88.7");
            }});
        }};
        normalizeInputForReference(Literal.of(map), objInfo);
        Symbol normalized = normalizeInputForReference(Literal.of(map), objInfo);
        assertThat(normalized, instanceOf(Literal.class));
        assertThat(((Literal<Map<String, Object>>) normalized).value().get("d"), Matchers.<Object>is(2.9d));
        assertThat(((Literal<Map<String, Object>>) normalized).value().get("inner_strict"),
            Matchers.<Object>is(new HashMap<String, Object>() {{
                                    put("double", -88.7d);
                                }}
            ));
    }

    @Test
    public void testNormalizeDynamicObjectLiteralWithAdditionalColumn() throws Exception {
        Reference objInfo = userTableInfo.getReference(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("d", 2.9d);
        map.put("half", "1.45");
        Symbol normalized = normalizeInputForReference(Literal.of(map), objInfo);
        assertThat(normalized, instanceOf(Literal.class));
        assertThat(((Literal) normalized).value(), Matchers.<Object>is(map)); // stays the same
    }

    @Test
    public void testNormalizeDynamicObjectWithRestrictedAdditionalColumn() throws Exception {
        expectedException.expect(InvalidColumnNameException.class);
        expectedException.expectMessage("contains a dot");
        Reference objInfo = userTableInfo.getReference(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("_invalid.column_name", 0);
        normalizeInputForReference(Literal.of(map), objInfo);
    }


    @Test(expected = ColumnUnknownException.class)
    public void testNormalizeStrictObjectLiteralWithAdditionalColumn() throws Exception {
        Reference objInfo = userTableInfo.getReference(new ColumnIdent("strict"));
        Map<String, Object> map = new HashMap<>();
        map.put("inner_d", 2.9d);
        map.put("half", "1.45");
        normalizeInputForReference(Literal.of(map), objInfo);
    }

    @Test(expected = ColumnUnknownException.class)
    public void testNormalizeStrictObjectLiteralWithAdditionalNestedColumn() throws Exception {
        Reference objInfo = userTableInfo.getReference(new ColumnIdent("strict"));
        Map<String, Object> map = new HashMap<>();
        map.put("inner_d", 2.9d);
        map.put("inner_map", new HashMap<String, Object>() {{
            put("much_inner", "yaw");
        }});
        normalizeInputForReference(Literal.of(map), objInfo);
    }

    @Test(expected = ColumnUnknownException.class)
    public void testNormalizeNestedStrictObjectLiteralWithAdditionalColumn() throws Exception {
        Reference objInfo = userTableInfo.getReference(new ColumnIdent("dyn"));

        Map<String, Object> map = new HashMap<>();
        map.put("inner_strict", new HashMap<String, Object>() {{
            put("double", 2.9d);
            put("half", "1.45");
        }});
        map.put("half", "1.45");
        normalizeInputForReference(Literal.of(map), objInfo);
    }

    @Test
    public void testNormalizeDynamicNewColumnTimestamp() throws Exception {
        Reference objInfo = userTableInfo.getReference(new ColumnIdent("dyn"));
        Map<String, Object> map = new HashMap<String, Object>() {{
            put("time", "1970-01-01T00:00:00");
        }};
        Literal<Map<String, Object>> literal = (Literal) normalizeInputForReference(
            Literal.of(map),
            objInfo);
        assertThat((String) literal.value().get("time"), is("1970-01-01T00:00:00"));
    }

    @Test
    public void testNormalizeIgnoredNewColumnTimestamp() throws Exception {
        Reference objInfo = userTableInfo.getReference(new ColumnIdent("ignored"));
        Map<String, Object> map = new HashMap<String, Object>() {{
            put("time", "1970-01-01T00:00:00");
        }};
        Literal<Map<String, Object>> literal = (Literal) normalizeInputForReference(
            Literal.of(map),
            objInfo);
        assertThat((String) literal.value().get("time"), is("1970-01-01T00:00:00"));
    }

    @Test
    public void testNormalizeDynamicNewColumnNoTimestamp() throws Exception {
        Reference objInfo = userTableInfo.getReference(new ColumnIdent("ignored"));
        Map<String, Object> map = new HashMap<String, Object>() {{
            put("no_time", "1970");
        }};
        Literal<Map<String, Object>> literal = (Literal) normalizeInputForReference(
            Literal.of(map),
            objInfo);
        assertThat((String) literal.value().get("no_time"), is("1970"));
    }

    @Test
    public void testNormalizeStringToNumberColumn() throws Exception {
        Reference objInfo = userTableInfo.getReference(new ColumnIdent("d"));
        Literal<BytesRef> stringDoubleLiteral = Literal.of("298.444");
        Literal literal = (Literal) normalizeInputForReference(
            stringDoubleLiteral, objInfo);
        assertThat(literal, isLiteral(298.444d, DataTypes.DOUBLE));
    }
}

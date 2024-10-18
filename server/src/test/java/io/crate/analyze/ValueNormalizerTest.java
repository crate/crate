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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.expressions.ValueNormalizer;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.InvalidColumnNameException;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.table.TableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;


public class ValueNormalizerTest extends CrateDummyClusterServiceUnitTest {

    private static final RelationName TEST_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "test1");
    private TableInfo userTableInfo;
    private EvaluatingNormalizer normalizer;

    private Symbol normalizeInputForReference(Symbol valueSymbol, Reference reference) {
        return ValueNormalizer.normalizeInputForReference(
            valueSymbol,
            reference,
            userTableInfo,
            s -> normalizer.normalize(s, CoordinatorTxnCtx.systemTransactionContext())
        );
    }

    @Before
    public void prepare() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table doc.test1 (" +
                      " id long primary key," +
                      " name string," +
                      " d double," +
                      " dyn_empty object," +
                      " dyn object as (" +
                      "  d double," +
                      "  inner_strict object(strict) as (" +
                      "   double double" +
                      "  )" +
                      " )," +
                      " strict object(strict) as (" +
                      "  inner_d double" +
                      " )," +
                      " ignored object(ignored)" +
                      ") " +
                      "clustered by (id)");
        userTableInfo = e.resolveTableInfo("doc.test1");
        normalizer = EvaluatingNormalizer.functionOnlyNormalizer(e.nodeCtx);
    }

    @Test
    public void testNormalizePrimitiveLiteral() throws Exception {
        SimpleReference ref = new SimpleReference(
            new ReferenceIdent(TEST_TABLE_IDENT, ColumnIdent.of("bool")), RowGranularity.DOC, DataTypes.BOOLEAN, 0, null
        );
        Literal<Boolean> trueLiteral = Literal.of(true);

        assertThat(normalizeInputForReference(trueLiteral, ref)).isEqualTo(trueLiteral);
        assertThat(normalizeInputForReference(Literal.of("true"), ref)).isEqualTo(trueLiteral);
        assertThat(normalizeInputForReference(Literal.of("false"), ref)).isEqualTo(Literal.of(false));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testNormalizeDynamicEmptyObjectLiteral() throws Exception {
        Reference objRef = userTableInfo.getReference(ColumnIdent.of("dyn_empty"));
        Map<String, Object> map = new HashMap<>();
        map.put("time", "2014-02-16T00:00:01");
        map.put("false", true);
        Literal<Map<String, Object>> normalized = (Literal) normalizeInputForReference(
            Literal.of(map), objRef);
        assertThat((String) normalized.value().get("time")).isEqualTo("2014-02-16T00:00:01");
        assertThat((Boolean) normalized.value().get("false")).isTrue();
    }

    @Test
    public void testNormalizeObjectLiteralInvalidNested() throws Exception {
        Reference objRef = userTableInfo.getReference(ColumnIdent.of("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("d", "2014-02-16T00:00:01");
        assertThatThrownBy(() -> normalizeInputForReference(Literal.of(map), objRef))
            .isExactlyInstanceOf(ColumnValidationException.class);
    }

    @Test
    public void testNormalizeObjectLiteralConvertFromString() throws Exception {
        Reference objInfo = userTableInfo.getReference(ColumnIdent.of("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("d", "2.9");

        Symbol normalized = normalizeInputForReference(Literal.of(map), objInfo);
        assertThat(normalized).isLiteral(Map.of("d", 2.9d));
    }

    @Test
    public void testNormalizeObjectLiteral() throws Exception {
        Reference objInfo = userTableInfo.getReference(ColumnIdent.of("dyn"));
        Map<String, Object> map = Map.of(
            "d", 2.9d,
            "inner_strict", Map.of(
                "double", "-88.7"));
        normalizeInputForReference(Literal.of(map), objInfo);
        Symbol normalized = normalizeInputForReference(Literal.of(map), objInfo);
        assertThat(normalized).isLiteral(Map.of("d", 2.9d, "inner_strict", Map.of("double", -88.7d)));
    }

    @Test
    public void testNormalizeDynamicObjectLiteralWithAdditionalColumn() throws Exception {
        Reference objInfo = userTableInfo.getReference(ColumnIdent.of("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("d", 2.9d);
        map.put("half", "1.45");
        Symbol normalized = normalizeInputForReference(Literal.of(map), objInfo);
        assertThat(normalized).isLiteral(map);
    }

    @Test
    public void testNormalizeDynamicObjectWithRestrictedAdditionalColumn() throws Exception {
        Reference objInfo = userTableInfo.getReference(ColumnIdent.of("dyn"));
        Map<String, Object> map = new HashMap<>();
        map.put("_invalid.column_name", 0);
        assertThatThrownBy(() -> normalizeInputForReference(Literal.of(map), objInfo))
            .isExactlyInstanceOf(InvalidColumnNameException.class)
            .hasMessage("\"_invalid.column_name\" contains a dot");
    }


    @Test
    public void testNormalizeStrictObjectLiteralWithAdditionalColumn() throws Exception {
        Reference objInfo = userTableInfo.getReference(ColumnIdent.of("strict"));
        Map<String, Object> map = new HashMap<>();
        map.put("inner_d", 2.9d);
        map.put("half", "1.45");
        assertThatThrownBy(() -> normalizeInputForReference(Literal.of(map), objInfo))
            .isExactlyInstanceOf(ColumnUnknownException.class);
    }

    @Test
    public void testNormalizeStrictObjectLiteralWithAdditionalNestedColumn() throws Exception {
        Reference objInfo = userTableInfo.getReference(ColumnIdent.of("strict"));
        Map<String, Object> map = Map.of(
            "inner_d", 2.9d,
            "inner_map", Map.of(
                "much_inner", "yaw"));
        assertThatThrownBy(() -> normalizeInputForReference(Literal.of(map), objInfo))
            .isExactlyInstanceOf(ColumnUnknownException.class);
    }

    @Test
    public void testNormalizeNestedStrictObjectLiteralWithAdditionalColumn() throws Exception {
        Reference objInfo = userTableInfo.getReference(ColumnIdent.of("dyn"));

        Map<String, Object> map = Map.of(
            "inner_strict", Map.of(
                    "double", 2.9d,
                    "half", "1.45"),
            "half", "1.45");
        assertThatThrownBy(() -> normalizeInputForReference(Literal.of(map), objInfo))
            .isExactlyInstanceOf(ColumnUnknownException.class);
    }

    @Test
    public void testNormalizeDynamicNewColumnTimestamp() throws Exception {
        Reference objInfo = userTableInfo.getReference(ColumnIdent.of("dyn"));
        Map<String, Object> map = Map.of("time", "1970-01-01T00:00:00");
        Symbol symbol = normalizeInputForReference(Literal.of(map), objInfo);
        assertThat(symbol).isLiteral(Map.of("time", "1970-01-01T00:00:00"));
    }

    @Test
    public void testNormalizeIgnoredNewColumnTimestamp() throws Exception {
        Reference objInfo = userTableInfo.getReference(ColumnIdent.of("ignored"));
        Map<String, Object> map = Map.of("time", "1970-01-01T00:00:00");
        Symbol symbol = normalizeInputForReference(Literal.of(map), objInfo);
        assertThat(symbol).isLiteral(Map.of("time", "1970-01-01T00:00:00"));
    }

    @Test
    public void testNormalizeDynamicNewColumnNoTimestamp() throws Exception {
        Reference objInfo = userTableInfo.getReference(ColumnIdent.of("ignored"));
        Map<String, Object> map = Map.of("no_time", "1970");
        Symbol symbol = normalizeInputForReference(Literal.of(map), objInfo);
        assertThat(symbol).isLiteral(Map.of("no_time", "1970"));
    }

    @Test
    public void testNormalizeStringToNumberColumn() throws Exception {
        Reference objInfo = userTableInfo.getReference(ColumnIdent.of("d"));
        Literal<String> stringDoubleLiteral = Literal.of("298.444");
        Symbol literal = normalizeInputForReference(stringDoubleLiteral, objInfo);
        assertThat(literal).isLiteral(298.444d, DataTypes.DOUBLE);
    }
}

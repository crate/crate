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
import static io.crate.testing.Asserts.isCollectionColumnType;
import static io.crate.testing.Asserts.isColumnDefinition;
import static io.crate.testing.Asserts.isColumnType;
import static io.crate.testing.Asserts.isEqualTo;
import static io.crate.testing.Asserts.isObjectColumnType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.Expression;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class SymbolToColumnDefinitionConverterTest extends CrateDummyClusterServiceUnitTest {

    private List<ColumnDefinition<Expression>> getAllColumnDefinitionsFrom(String createTableStmt) throws IOException {
        var e = SQLExecutor.builder(clusterService).addTable(createTableStmt).build();
        AnalyzedRelation analyzedRelation = e.analyze("select * from tbl");
        return Lists2.map(analyzedRelation.outputs(), Symbols::toColumnDefinition);
    }

    @Test
    public void testPrimitiveTypeToColumnDefinition() throws IOException {

        String createTableStmt =
            "create table tbl (" +
            "   col_boolean boolean," +
            "   col_integer integer," +
            "   col_bigint bigint," +
            "   col_smallint smallint," +
            "   col_double_precision double precision," +
            "   col_real real," +
            "   col_char \"char\"," +
            "   col_text text," +
            "   col_varchar varchar," +
            "   col_varchar_len_6 varchar(6)," +
            "   col_ip ip," +
            "   col_timestamp_without_time_zone timestamp without time zone," +
            "   col_timestamp_with_time_zone timestamp with time zone" +
            ")";
        var actual = getAllColumnDefinitionsFrom(createTableStmt);

        assertThat(actual).satisfiesExactlyInAnyOrder(
                c -> assertThat(c).isColumnDefinition("col_boolean", isColumnType(DataTypes.BOOLEAN.getName())),
                c -> assertThat(c).isColumnDefinition("col_integer", isColumnType(DataTypes.INTEGER.getName())),
                c -> assertThat(c).isColumnDefinition("col_bigint", isColumnType(DataTypes.LONG.getName())),
                c -> assertThat(c).isColumnDefinition("col_smallint", isColumnType(DataTypes.SHORT.getName())),
                c -> assertThat(c).isColumnDefinition("col_double_precision", isColumnType(DataTypes.DOUBLE.getName())),
                c -> assertThat(c).isColumnDefinition("col_real", isColumnType(DataTypes.FLOAT.getName())),
                c -> assertThat(c).isColumnDefinition("col_char", isColumnType(DataTypes.BYTE.getName())),
                c -> assertThat(c).isColumnDefinition("col_text", isColumnType(DataTypes.STRING.getName())),
                c -> assertThat(c).isColumnDefinition("col_varchar", isColumnType(DataTypes.STRING.getName())),
                c -> assertThat(c).isColumnDefinition("col_varchar_len_6", isColumnType("varchar", 6)),
                c -> assertThat(c).isColumnDefinition("col_ip", isColumnType(DataTypes.IP.getName())),
                c -> assertThat(c).isColumnDefinition("col_timestamp_without_time_zone", isColumnType(DataTypes.TIMESTAMP.getName())),
                c -> assertThat(c).isColumnDefinition("col_timestamp_with_time_zone", isColumnType(DataTypes.TIMESTAMPZ.getName()))
        );
    }

    @Test
    public void testGeographicTypeToColumnDefinition() throws IOException {
        String createTableStmt =
            "create table tbl (" +
            "   col_geo_point geo_point," +
            "   col_geo_shape geo_shape" +
            ")";
        var actual = getAllColumnDefinitionsFrom(createTableStmt);

        assertThat(actual).satisfiesExactlyInAnyOrder(
                c -> assertThat(c).isColumnDefinition(
                    "col_geo_point",
                    isColumnType(DataTypes.GEO_POINT.getName())),
                c -> assertThat(c).isColumnDefinition(
                    "col_geo_shape",
                    isColumnType(DataTypes.GEO_SHAPE.getName()))
        );
    }

    @Test
    public void test_objects_column_policies_are_preserved() throws IOException {
        String createTableStmt =
            "create table tbl (" +
            "   col_strict_object object(STRICT)," +
            "   col_dynamic_object object(DYNAMIC)," +
            "   col_ignored_object object(IGNORED)" +
            ")";
        var actual = getAllColumnDefinitionsFrom(createTableStmt);

        assertThat(actual).satisfiesExactlyInAnyOrder(
            c -> assertThat(c).isColumnDefinition(
                    "col_strict_object",
                    isObjectColumnType(DataTypes.UNTYPED_OBJECT.getName(),
                                       isEqualTo(ColumnPolicy.STRICT))),
            c -> assertThat(c).isColumnDefinition(
                    "col_dynamic_object",
                    isObjectColumnType(DataTypes.UNTYPED_OBJECT.getName(),
                                       isEqualTo(ColumnPolicy.DYNAMIC))),
            c -> assertThat(c).isColumnDefinition(
                    "col_ignored_object",
                    isObjectColumnType(DataTypes.UNTYPED_OBJECT.getName(),
                                       isEqualTo(ColumnPolicy.IGNORED)))
        );
    }

    @Test
    public void testEntireObjectToColumDefinition() throws IOException {
        String createTableStmt =
            "create table tbl (" +
            "   col_default_object object as (" +
            "       col_nested_integer integer," +
            "       col_nested_object object as (" +
            "           col_nested_timestamp_with_time_zone timestamp with time zone" +
            "       )" +
            "   )" +
            ")";
        var e = SQLExecutor.builder(clusterService).addTable(createTableStmt).build();
        AnalyzedRelation analyzedRelation = e.analyze(
            "select col_default_object from tbl"
        );
        var actual = Lists2.map(analyzedRelation.outputs(), Symbols::toColumnDefinition);

        assertThat(actual.get(0))
            .isColumnDefinition(
                "col_default_object",
                isObjectColumnType(
                    DataTypes.UNTYPED_OBJECT.getName(),
                    isEqualTo(ColumnPolicy.DYNAMIC),
                    x -> assertThat(x).satisfiesExactlyInAnyOrder(
                        c -> assertThat(c).isColumnDefinition(
                            "col_nested_integer",
                            isColumnType(DataTypes.INTEGER.getName())),
                        c -> assertThat(c).isColumnDefinition(
                            "col_nested_object",
                            isObjectColumnType(
                                DataTypes.UNTYPED_OBJECT.getName(),
                                isEqualTo(ColumnPolicy.DYNAMIC),
                                a -> assertThat(a).satisfiesExactly(
                                    b -> assertThat(b).isColumnDefinition(
                                        "col_nested_timestamp_with_time_zone",
                                        isColumnType(DataTypes.TIMESTAMPZ.getName())))))))
            );
    }

    @Test
    public void testNestedObjectTypeToColumDefinition() throws IOException {
        String createTableStmt =
            "create table tbl (" +
            "   col_default_object object as (" +
            "       col_nested_integer integer," +
            "       col_nested_object object as (" +
            "           col_nested_timestamp_with_time_zone timestamp with time zone" +
            "       )" +
            "   )" +
            ")";
        var actual = getAllColumnDefinitionsFrom(createTableStmt);

        assertThat(actual.get(0))
            .isColumnDefinition(
                "col_default_object",
                isObjectColumnType(
                    DataTypes.UNTYPED_OBJECT.getName(),
                    isEqualTo(ColumnPolicy.DYNAMIC),
                    x -> assertThat(x).satisfiesExactlyInAnyOrder(
                        isColumnDefinition(
                            "col_nested_integer",
                            isColumnType(DataTypes.INTEGER.getName())),
                        isColumnDefinition(
                            "col_nested_object",
                            isObjectColumnType(
                                DataTypes.UNTYPED_OBJECT.getName(),
                                isEqualTo(ColumnPolicy.DYNAMIC),
                                a -> assertThat(a).satisfiesExactly(
                                    isColumnDefinition(
                                        "col_nested_timestamp_with_time_zone",
                                        isColumnType(DataTypes.TIMESTAMPZ.getName())))))))
            );
    }

    @Test
    public void testSubFieldOfObjectTypeToColumnDefinition() throws IOException {
        String createTableStmt =
            "create table tbl (" +
            "   col_default_object object as (" +
            "       col_nested_integer integer," +
            "       col_nested_object object as (" +
            "           col_nested_timestamp_with_time_zone timestamp with time zone" +
            "       )" +
            "   )" +
            ")";
        var e = SQLExecutor.builder(clusterService).addTable(createTableStmt).build();
        String selectStmt =
            "select " +
            "   col_default_object['col_nested_integer'], " +
            "   col_default_object['col_nested_object']['col_nested_timestamp_with_time_zone'], " +
            "   col_default_object['col_nested_object']" +
            "from tbl";
        var analyzedRelation = e.analyze(selectStmt);
        var actual =
            Lists2.map(Objects.requireNonNull(analyzedRelation.outputs()), Symbols::toColumnDefinition);

        assertThat(actual).satisfiesExactlyInAnyOrder(
            c -> assertThat(c).isColumnDefinition(
                    "col_default_object['col_nested_integer']",
                    isColumnType(DataTypes.INTEGER.getName())),
            c -> assertThat(c).isColumnDefinition(
                    "col_default_object['col_nested_object']['col_nested_timestamp_with_time_zone']",
                    isColumnType(DataTypes.TIMESTAMPZ.getName())),
            c -> assertThat(c).isColumnDefinition(
                    "col_default_object['col_nested_object']",
                    isObjectColumnType(
                        DataTypes.UNTYPED_OBJECT.getName(),
                        isEqualTo(ColumnPolicy.DYNAMIC),
                        a -> assertThat(a).satisfiesExactly(
                            isColumnDefinition(
                                "col_nested_timestamp_with_time_zone",
                                isColumnType(DataTypes.TIMESTAMPZ.getName())))))
        );
    }

    @Test
    public void testArrayTypes() throws IOException {
        String createTableStmt =
            "create table tbl (" +
            "   array_boolean boolean[]," +
            "   Array_bigint bigint[]," +
            "   array_text text[]," +
            "   array_ip ip[]," +
            "   array_double_precision double precision[]," +
            "   array_char \"char\"[]," +
            "   array_varchar_len_6 varchar(6)[]," +
            "   array_timestamp_with_time_zone timestamp with time zone[]," +
            "   array_timestamp_without_time_zone timestamp without time zone[]," +
            "   array_ignored_object object(IGNORED)[]," +
            "   array_geo_point geo_point[]" +
            ")";
        var actual = getAllColumnDefinitionsFrom(createTableStmt);

        assertThat(actual).satisfiesExactlyInAnyOrder(
            c -> assertThat(c).isColumnDefinition("array_boolean",
                                   isCollectionColumnType(ArrayType.NAME.toUpperCase(),
                                                          isColumnType(DataTypes.BOOLEAN.getName()))),
            c -> assertThat(c).isColumnDefinition("array_bigint",
                                   isCollectionColumnType(ArrayType.NAME.toUpperCase(),
                                                          isColumnType(DataTypes.LONG.getName()))),
            c -> assertThat(c).isColumnDefinition("array_text",
                                   isCollectionColumnType(ArrayType.NAME.toUpperCase(),
                                                          isColumnType(DataTypes.STRING.getName()))),
            c -> assertThat(c).isColumnDefinition("array_ip",
                                   isCollectionColumnType(ArrayType.NAME.toUpperCase(),
                                                          isColumnType(DataTypes.IP.getName()))),
            c -> assertThat(c).isColumnDefinition("array_double_precision",
                                   isCollectionColumnType(ArrayType.NAME.toUpperCase(),
                                                          isColumnType(DataTypes.DOUBLE.getName()))),
            c -> assertThat(c).isColumnDefinition("array_char",
                                   isCollectionColumnType(ArrayType.NAME.toUpperCase(),
                                                          isColumnType(DataTypes.BYTE.getName()))),
            c -> assertThat(c).isColumnDefinition("array_varchar_len_6",
                                   isCollectionColumnType(ArrayType.NAME.toUpperCase(),
                                                          isColumnType("varchar", 6))),
            c -> assertThat(c).isColumnDefinition("array_timestamp_with_time_zone",
                                   isCollectionColumnType(ArrayType.NAME.toUpperCase(),
                                                          isColumnType(DataTypes.TIMESTAMPZ.getName()))),
            c -> assertThat(c).isColumnDefinition("array_timestamp_without_time_zone",
                                   isCollectionColumnType(ArrayType.NAME.toUpperCase(),
                                                          isColumnType(DataTypes.TIMESTAMP.getName()))),
            c -> assertThat(c).isColumnDefinition("array_ignored_object",
                                   isCollectionColumnType(ArrayType.NAME.toUpperCase(),
                                                          isObjectColumnType(
                                                              DataTypes.UNTYPED_OBJECT.getName(),
                                                              isEqualTo(ColumnPolicy.IGNORED)))),
            c -> assertThat(c).isColumnDefinition("array_geo_point",
                                   isCollectionColumnType(ArrayType.NAME.toUpperCase(),
                                                          isColumnType(DataTypes.GEO_POINT.getName())))
        );
    }

    @Test
    public void testAliasedNameToColumnDefinition() throws IOException {
        String createTableStmt =
            "create table tbl (" +
            "   col_default_object object as (" +
            "       col_nested_integer integer," +
            "       col_nested_object object as (" +
            "           col_nested_timestamp_with_time_zone timestamp with time zone" +
            "       )" +
            "   )" +
            ")";

        var e = SQLExecutor.builder(clusterService).addTable(createTableStmt).build();
        String selectStmt =
            "select " +
            "   col_default_object['col_nested_integer'] as col1, " +
            "   col_default_object['col_nested_object']['col_nested_timestamp_with_time_zone'] as col2, " +
            "   col_default_object['col_nested_object'] as col3 " +
            "from tbl";
        var analyzedRelation = e.analyze(selectStmt);
        var actual =
            Lists2.map(Objects.requireNonNull(analyzedRelation.outputs()), Symbols::toColumnDefinition);

        assertThat(actual).satisfiesExactlyInAnyOrder(
            c -> assertThat(c).isColumnDefinition(
                "col1",
                isColumnType(DataTypes.INTEGER.getName())),
            c -> assertThat(c).isColumnDefinition(
                "col2",
                isColumnType(DataTypes.TIMESTAMPZ.getName())),
            c -> assertThat(c).isColumnDefinition(
                "col3",
                isObjectColumnType(
                    DataTypes.UNTYPED_OBJECT.getName(),
                    isEqualTo(ColumnPolicy.DYNAMIC),
                    a -> assertThat(a).satisfiesExactly(
                        isColumnDefinition(
                            "col_nested_timestamp_with_time_zone",
                            isColumnType(DataTypes.TIMESTAMPZ.getName())))))
        );
    }

    @Test
    public void testSymbolFromViewToColumnDefinition() throws IOException {
        String createTableStmt =
            "create table tbl (" +
            "   col_default_object object as (" +
            "       col_nested_integer integer," +
            "       col_nested_object object as (" +
            "           col_nested_timestamp_with_time_zone timestamp with time zone" +
            "       )" +
            "   )" +
            ")";

        var e = SQLExecutor
            .builder(clusterService)
            .addTable(createTableStmt)
            .addView(new RelationName("doc", "tbl_view"), "select * from doc.tbl")
            .build();
        String selectStmt =
            "select " +
            "   col_default_object['col_nested_integer'] as col1, " +
            "   col_default_object['col_nested_object']['col_nested_timestamp_with_time_zone'] as col2, " +
            "   col_default_object['col_nested_object'] as col3 " +
            "from tbl_view";
        var analyzedRelation = e.analyze(selectStmt);
        var actual =
            Lists2.map(Objects.requireNonNull(analyzedRelation.outputs()), Symbols::toColumnDefinition);

        assertThat(actual).satisfiesExactlyInAnyOrder(
            c -> assertThat(c).isColumnDefinition(
                    "col1",
                    isColumnType(DataTypes.INTEGER.getName())),
            c -> assertThat(c).isColumnDefinition(
                    "col2",
                    isColumnType(DataTypes.TIMESTAMPZ.getName())),
            c -> assertThat(c).isColumnDefinition(
                    "col3",
                    isObjectColumnType(
                        DataTypes.UNTYPED_OBJECT.getName(),
                        isEqualTo(ColumnPolicy.DYNAMIC),
                        a -> assertThat(a).satisfiesExactly(
                            isColumnDefinition(
                                "col_nested_timestamp_with_time_zone",
                                isColumnType(DataTypes.TIMESTAMPZ.getName())))))
        );
    }

    @Test
    public void testScopedNameToColumnDefinition() throws IOException {
        String createTableStmt =
            "create table tbl (" +
            "   col_default_object object as (" +
            "       col_nested_integer integer," +
            "       col_nested_object object as (" +
            "           col_nested_timestamp_with_time_zone timestamp with time zone" +
            "       )" +
            "   )" +
            ")";
        var e = SQLExecutor.builder(clusterService).addTable(createTableStmt).build();
        String selectStmt =
            "select A.col_default_object['col_nested_integer'], " +
            "   A.col_default_object['col_nested_object']['col_nested_timestamp_with_time_zone'], " +
            "   A.col_default_object['col_nested_object'] " +
            "from " +
            "   (select " +
            "       col_default_object['col_nested_integer'], " +
            "       col_default_object['col_nested_object']['col_nested_timestamp_with_time_zone'], " +
            "       col_default_object['col_nested_object']" +
            "   from tbl) as A";
        var analyzedRelation = e.analyze(selectStmt);
        var actual =
            Lists2.map(Objects.requireNonNull(analyzedRelation.outputs()), Symbols::toColumnDefinition);

        assertThat(actual).satisfiesExactlyInAnyOrder(
            c -> assertThat(c).isColumnDefinition(
                    "col_default_object['col_nested_integer']",
                    isColumnType(DataTypes.INTEGER.getName())),
            c -> assertThat(c).isColumnDefinition(
                    "col_default_object['col_nested_object']['col_nested_timestamp_with_time_zone']",
                    isColumnType(DataTypes.TIMESTAMPZ.getName())),
            c -> assertThat(c).isColumnDefinition(
                    "col_default_object['col_nested_object']",
                    isObjectColumnType(
                        DataTypes.UNTYPED_OBJECT.getName(),
                        isEqualTo(ColumnPolicy.DYNAMIC),
                        a -> assertThat(a).satisfiesExactly(
                            isColumnDefinition(
                                "col_nested_timestamp_with_time_zone",
                                isColumnType(DataTypes.TIMESTAMPZ.getName())))))
        );
    }

    @Test
    public void testTypeCastedSymbolToColumnDefinition() {
        //check for naming of the target columns
        String selectStmt =
            "select cast([0,1,5] as array(boolean)) AS active_threads, " +
            "   cast(port['http']as boolean) from sys.nodes limit 1 ";
        var e = SQLExecutor.builder(clusterService).build();
        var analyzedRelation = e.analyze(selectStmt);
        var actual = Lists2.map(Objects.requireNonNull(analyzedRelation.outputs()), Symbols::toColumnDefinition);

        assertThat(actual).satisfiesExactlyInAnyOrder(
            c -> assertThat(c).isColumnDefinition(
                "cast(port['http'] AS boolean)",
                isColumnType(DataTypes.BOOLEAN.getName())),
            c -> assertThat(c).isColumnDefinition(
                "active_threads",
                isCollectionColumnType(
                    ArrayType.NAME.toUpperCase(),
                    isColumnType(DataTypes.BOOLEAN.getName())))
        );
    }
}

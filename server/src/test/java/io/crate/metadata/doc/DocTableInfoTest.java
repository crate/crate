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

package io.crate.metadata.doc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.VoidReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class DocTableInfoTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testGetColumnInfo() throws Exception {
        RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy");

        ColumnIdent columnIdent = new ColumnIdent("o", List.of());
        DocTableInfo info = new DocTableInfo(
            relationName,
            Map.of(
                columnIdent,
                new SimpleReference(
                    new ReferenceIdent(relationName, columnIdent),
                    RowGranularity.DOC,
                    DataTypes.UNTYPED_OBJECT,
                    1,
                    null
                )
            ),
            Map.of(),
            Map.of(),
            null,
            List.of(),
            List.of(),
            null,
            true,
            new String[0],
            new String[0],
            5,
            "0",
            Settings.EMPTY,
            List.of(),
            List.of(),
            ColumnPolicy.DYNAMIC,
            Version.CURRENT,
            null,
            false,
            Operation.ALL
        );
        final ColumnIdent col = new ColumnIdent("o", List.of("foobar"));
        Reference foobar = info.getReference(col);
        assertThat(foobar).isNull();

        // forWrite: false, errorOnUnknownObjectKey: true, parentPolicy: dynamic
        DynamicReference reference = info.getDynamic(col, false, true);
        assertThat(reference).isNull();

        // forWrite: true, errorOnUnknownObjectKey: true, parentPolicy: dynamic
        reference = info.getDynamic(col, true, true);
        assertThat(reference).isNotNull();
        assertThat(reference.valueType()).isEqualTo(DataTypes.UNDEFINED);

        // forWrite: true, errorOnUnknownObjectKey: false, parentPolicy: dynamic
        reference = info.getDynamic(col, true, false);
        assertThat(reference).isNotNull();
        assertThat(reference.valueType()).isEqualTo(DataTypes.UNDEFINED);

        // forWrite: false, errorOnUnknownObjectKey: false, parentPolicy: dynamic
        reference = info.getDynamic(col, false, false);
        assertThat(reference).isNotNull();
        assertThat(reference).isInstanceOf(VoidReference.class);
        assertThat(reference.valueType()).isEqualTo(DataTypes.UNDEFINED);
    }

    @Test
    public void testGetColumnInfoStrictParent() throws Exception {
        RelationName dummy = new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy");
        ColumnIdent column = new ColumnIdent("foobar");
        ReferenceIdent foobarIdent = new ReferenceIdent(dummy, column);
        SimpleReference strictParent = new SimpleReference(
            foobarIdent,
            RowGranularity.DOC,
            DataTypes.UNTYPED_OBJECT,
            ColumnPolicy.STRICT,
            IndexType.PLAIN,
            true,
            false,
            1,
            COLUMN_OID_UNASSIGNED,
            false,
            null
        );

        Map<ColumnIdent, Reference> references = Map.of(column, strictParent);

        DocTableInfo info = new DocTableInfo(
            dummy,
            references,
            Map.of(),
            Map.of(),
            null,
            List.of(),
            List.of(),
            null,
            true,
            new String[0],
            new String[0],
            5,
            "0",
            Settings.EMPTY,
            List.of(),
            List.of(),
            ColumnPolicy.DYNAMIC,
            Version.CURRENT,
            null,
            false,
            Operation.ALL
        );

        final ColumnIdent columnIdent = new ColumnIdent("foobar", Arrays.asList("foo", "bar"));
        assertThat(info.getReference(columnIdent)).isNull();

        // forWrite: false, errorOnUnknownObjectKey: true, parentPolicy: strict
        assertThat(info.getDynamic(columnIdent, false, true)).isNull();

        // forWrite: true, errorOnUnknownObjectKey: true, parentPolicy: strict
        Assertions.assertThatThrownBy(() -> info.getDynamic(columnIdent, true, true))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessageContaining("Column foobar['foo']['bar'] unknown");

        // forWrite: false, errorOnUnknownObjectKey: false, parentPolicy: strict
        assertThat(info.getDynamic(columnIdent, false, false)).isNull();

        // forWrite: true, errorOnUnknownObjectKey: false, parentPolicy: strict
        Assertions.assertThatThrownBy(() -> info.getDynamic(columnIdent, true, false))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessageContaining("Column foobar['foo']['bar'] unknown");

        final ColumnIdent columnIdent2 = new ColumnIdent("foobar", Collections.singletonList("foo"));
        assertThat(info.getReference(columnIdent2)).isNull();

        // forWrite: false, errorOnUnknownObjectKey: true, parentPolicy: strict
        assertThat(info.getDynamic(columnIdent2, false, true)).isNull();

        // forWrite: true, errorOnUnknownObjectKey: true, parentPolicy: strict
        Assertions.assertThatThrownBy(() -> info.getDynamic(columnIdent2, true, true))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessageContaining("Column foobar['foo'] unknown");

        // forWrite: false, errorOnUnknownObjectKey: false, parentPolicy: strict
        assertThat(info.getDynamic(columnIdent2, false, false)).isNull();

        // forWrite: true, errorOnUnknownObjectKey: false, parentPolicy: strict
        Assertions.assertThatThrownBy(() -> info.getDynamic(columnIdent2, true, false))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessageContaining("Column foobar['foo'] unknown");

        Reference colInfo = info.getReference(column);
        assertThat(colInfo).isNotNull();
    }

    @Test
    public void test_can_retrieve_all_parents_of_nested_object_column() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o1 object as (o2 object as (x int)))")
            .build();

        TableInfo table = e.resolveTableInfo("tbl");
        Iterable<Reference> parents = table.getParents(new ColumnIdent("o1", List.of("o2", "x")));
        assertThat(parents).containsExactly(
            table.getReference(new ColumnIdent("o1", "o2")),
            table.getReference(new ColumnIdent("o1"))
        );
    }

    @Test
    public void test_version_created_is_read_from_partitioned_template() throws Exception {
        var customSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_5_0_0)
            .build();
        var e = SQLExecutor.builder(clusterService)
            .addPartitionedTable("CREATE TABLE p1 (id INT, p INT) PARTITIONED BY (p)", customSettings)
            .build();

        DocTableInfo tableInfo = e.resolveTableInfo("p1");
        assertThat(IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(tableInfo.parameters())).isEqualTo(Version.V_5_0_0);
    }

    @Test
    public void test_version_created_is_set_to_current_version_if_unavailable_at_partitioned_template() throws Exception {
        var customSettings = Settings.builder()
            .build();
        var e = SQLExecutor.builder(clusterService)
            .addPartitionedTable("CREATE TABLE p1 (id INT, p INT) PARTITIONED BY (p)", customSettings)
            .build();

        DocTableInfo tableInfo = e.resolveTableInfo("p1");
        assertThat(IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(tableInfo.parameters())).isEqualTo(Version.CURRENT);
    }

    @Test
    public void test_dropped_columns_are_included_in_oid_to_column_map() throws Exception {
        RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy");

        ColumnIdent a = new ColumnIdent("a", List.of());
        ColumnIdent b = new ColumnIdent("b", List.of());
        DocTableInfo info = new DocTableInfo(
                relationName,
                Map.of(
                    a,
                    new SimpleReference(
                            new ReferenceIdent(relationName, a),
                            RowGranularity.DOC,
                            DataTypes.INTEGER,
                            ColumnPolicy.DYNAMIC,
                            IndexType.PLAIN,
                            true,
                            false,
                            1,
                            1,
                            false,
                            null
                    ),
                    b,
                    new SimpleReference(
                            new ReferenceIdent(relationName, b),
                            RowGranularity.DOC,
                            DataTypes.INTEGER,
                            ColumnPolicy.DYNAMIC,
                            IndexType.PLAIN,
                            true,
                            false,
                            2,
                            2,
                            true,
                            null
                    )
                ),
                Map.of(),
                Map.of(),
                null,
                List.of(),
                List.of(),
                null,
                true,
                new String[0],
                new String[0],
                5,
                "0",
                Settings.EMPTY,
                List.of(),
                List.of(),
                ColumnPolicy.DYNAMIC,
                Version.CURRENT,
                null,
                false,
                Operation.ALL
        );

        assertThat(info.lookupNameBySourceKey().apply("2")).isEqualTo("b");
    }
}

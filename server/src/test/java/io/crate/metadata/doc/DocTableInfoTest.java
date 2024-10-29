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

import static io.crate.testing.Asserts.assertThat;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.analyze.DropColumn;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.VoidReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferences;
import io.crate.metadata.IndexReference;
import io.crate.metadata.IndexType;
import io.crate.metadata.PartitionName;
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
import io.crate.testing.UseRandomizedSchema;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class DocTableInfoTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testGetColumnInfo() throws Exception {
        RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy");

        ColumnIdent columnIdent = ColumnIdent.of("o", List.of());
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
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                .build(),
            List.of(),
            ColumnPolicy.DYNAMIC,
            Version.CURRENT,
            null,
            false,
            Operation.ALL,
            0
        );
        final ColumnIdent col = ColumnIdent.of("o", List.of("foobar"));
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
        ColumnIdent column = ColumnIdent.of("foobar");
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
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                .build(),
            List.of(),
            ColumnPolicy.DYNAMIC,
            Version.CURRENT,
            null,
            false,
            Operation.ALL,
            0
        );

        final ColumnIdent columnIdent = ColumnIdent.of("foobar", Arrays.asList("foo", "bar"));
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

        final ColumnIdent columnIdent2 = ColumnIdent.of("foobar", Collections.singletonList("foo"));
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
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (o1 object as (o2 object as (x int)))");

        TableInfo table = e.resolveTableInfo("tbl");
        Iterable<Reference> parents = table.getParents(ColumnIdent.of("o1", List.of("o2", "x")));
        assertThat(parents).containsExactly(
            table.getReference(ColumnIdent.of("o1", "o2")),
            table.getReference(ColumnIdent.of("o1"))
        );
    }

    @Test
    public void test_version_created_is_read_from_partitioned_template() throws Exception {
        var customSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_5_0_0)
            .build();
        var e = SQLExecutor.of(clusterService)
            .addPartitionedTable("CREATE TABLE p1 (id INT, p INT) PARTITIONED BY (p)", customSettings);

        DocTableInfo tableInfo = e.resolveTableInfo("p1");
        assertThat(IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(tableInfo.parameters())).isEqualTo(Version.V_5_0_0);
    }

    @Test
    public void test_version_created_is_set_to_current_version_if_unavailable_at_partitioned_template() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addPartitionedTable("CREATE TABLE p1 (id INT, p INT) PARTITIONED BY (p)", Settings.EMPTY);

        DocTableInfo tableInfo = e.resolveTableInfo("p1");
        assertThat(IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(tableInfo.parameters())).isEqualTo(Version.CURRENT);
    }

    @Test
    public void test_dropped_columns_are_included_in_oid_to_column_map() throws Exception {
        RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy");

        ColumnIdent a = ColumnIdent.of("a", List.of());
        ColumnIdent b = ColumnIdent.of("b", List.of());
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
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                    .build(),
                List.of(),
                ColumnPolicy.DYNAMIC,
                Version.CURRENT,
                null,
                false,
                Operation.ALL,
                0
        );

        assertThat(info.lookupNameBySourceKey().apply("2")).isEqualTo("b");
    }

    @Test
    public void test_drop_column_updates_type_of_parent_ref() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (o1 object as (o2 object as (x int)))");
        DocTableInfo table = e.resolveTableInfo("tbl");
        ColumnIdent o1o2 = ColumnIdent.of("o1", "o2");
        Reference o1o2Ref = table.getReference(o1o2);
        DropColumn dropColumn = new DropColumn(o1o2Ref, true);
        DocTableInfo updatedTable = table.dropColumns(List.of(dropColumn));

        Reference o1Ref = updatedTable.getReference(ColumnIdent.of("o1"));
        assertThat(o1Ref.valueType()).isExactlyInstanceOf(ObjectType.class);
        ObjectType objectType = ((ObjectType) o1Ref.valueType());
        assertThat(objectType.innerTypes()).isEmpty();
    }

    @Test
    public void test_drop_column_after_drop_column_preserves_previous_dropped_columns() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int, y int, z int)");
        DocTableInfo table1 = e.resolveTableInfo("tbl");
        Reference xref = table1.getReference(ColumnIdent.of("x"));
        Reference yref = table1.getReference(ColumnIdent.of("y"));
        DocTableInfo table2 = table1.dropColumns(List.of(new DropColumn(xref, true)));
        assertThat(table2.droppedColumns()).satisfiesExactlyInAnyOrder(
            x -> assertThat(x).hasName("x")
        );
        DocTableInfo table3 = table2.dropColumns(List.of(new DropColumn(yref, true)));
        assertThat(table3.droppedColumns()).satisfiesExactlyInAnyOrder(
            x -> assertThat(x).hasName("x"),
            x -> assertThat(x).hasName("y")
        );
    }

    @Test
    public void test_write_to_preserves_indices() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(
                """
                create table tbl (
                    id int primary key,
                    name text,
                    description text index using fulltext with (analyzer = 'simple'),
                    index name_ft using fulltext (name) with (analyzer = 'standard')
                )
                """
            );
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        ClusterState state = clusterService.state();
        Metadata metadata = state.metadata();
        Metadata.Builder builder = new Metadata.Builder(metadata);
        tbl.writeTo(metadata, builder);

        DocTableInfoFactory docTableInfoFactory = new DocTableInfoFactory(e.nodeCtx);
        DocTableInfo tbl2 = docTableInfoFactory.create(tbl.ident(), builder.build());

        Reference description = tbl2.getReference(ColumnIdent.of("description"));
        assertThat(description)
            .isExactlyInstanceOf(IndexReference.class)
            .hasName("description")
            .hasAnalyzer("simple");

        IndexReference indexColumn = tbl2.indexColumn(ColumnIdent.of("name_ft"));
        assertThat(indexColumn).isNotNull();
        assertThat(indexColumn.analyzer()).isEqualTo("standard");
        assertThat(indexColumn.columns()).satisfiesExactly(
            x -> assertThat(x).hasName("name")
        );
    }

    @Test
    public void test_can_add_column_to_table() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int, point object as (x int))");
        DocTableInfo table1 = e.resolveTableInfo("tbl");
        Reference xref = table1.getReference(ColumnIdent.of("x"));
        Reference pointRef = table1.getReference(ColumnIdent.of("point"));
        SimpleReference newReference = new SimpleReference(
            new ReferenceIdent(table1.ident(), "y"),
            RowGranularity.DOC,
            DataTypes.LONG,
            -1,
            null
        );
        AtomicLong oidSupplier = new AtomicLong(2);
        DocTableInfo table2 = table1.addColumns(
            e.nodeCtx,
            oidSupplier::incrementAndGet,
            List.of(newReference),
            new IntArrayList(),
            Map.of());
        assertThat(table2.columns()).satisfiesExactly(
            x -> assertThat(x)
                .hasName("x")
                .hasType(DataTypes.INTEGER)
                .hasPosition(1)
                .isSameAs(xref),
            x -> assertThat(x)
                .hasName("point")
                .hasPosition(2)
                .hasType(pointRef.valueType())
                .isSameAs(pointRef),
            x -> assertThat(x)
                .hasName("y")
                .hasPosition(4)
                .hasType(DataTypes.LONG)
        );


        SimpleReference pointY = new SimpleReference(
            new ReferenceIdent(table1.ident(), ColumnIdent.of("point", "y")),
            RowGranularity.DOC,
            DataTypes.INTEGER,
            -1,
            null
        );
        DocTableInfo table3 = table2.addColumns(
            e.nodeCtx,
            oidSupplier::incrementAndGet,
            List.of(pointY),
            new IntArrayList(),
            Map.of());
        Reference newPointRef = table3.getReference(ColumnIdent.of("point"));
        assertThat(newPointRef.valueType()).isExactlyInstanceOf(ObjectType.class);
        DataType<?> yInnerType = ((ObjectType) newPointRef.valueType()).innerType("y");
        assertThat(yInnerType).isEqualTo(DataTypes.INTEGER);
    }

    @Test
    public void test_cannot_add_child_column_without_defining_parents() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");
        DocTableInfo table = e.resolveTableInfo("tbl");
        Reference ox = new SimpleReference(
            new ReferenceIdent(table.ident(), ColumnIdent.of("o", "x")),
            RowGranularity.DOC,
            DataTypes.INTEGER,
            -1,
            null
        );
        assertThatThrownBy(() ->
            table.addColumns(
                e.nodeCtx,
                () -> 2,
                List.of(ox),
                new IntArrayList(),
                Map.of())
        ).isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot create parents of new column implicitly. `o` is undefined");
    }

    @Test
    public void test_add_column_fixes_inner_types_of_all_its_parents() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (o object as (o object as (b1 int), a1 int))");
        DocTableInfo table = e.resolveTableInfo("tbl");
        SimpleReference newReference1 = new SimpleReference(
            new ReferenceIdent(table.ident(), ColumnIdent.of("o", List.of("o", "o", "c1"))),
            RowGranularity.DOC,
            DataTypes.INTEGER,
            -1,
            null
        );
        SimpleReference newReference2 = new SimpleReference(
            new ReferenceIdent(table.ident(), ColumnIdent.of("o", List.of("o", "o"))),
            RowGranularity.DOC,
            DataTypes.UNTYPED_OBJECT,
            -1,
            null
        );
        SimpleReference newReference3 = new SimpleReference(
            new ReferenceIdent(table.ident(), ColumnIdent.of("o", List.of("o", "b2"))),
            RowGranularity.DOC,
            DataTypes.INTEGER,
            -1,
            null
        );
        DocTableInfo newTable = table.addColumns(
            e.nodeCtx,
            () -> 10, // any oid
            List.of(newReference1, newReference2, newReference3),
            new IntArrayList(),
            Map.of()
        );

        var oooType = ObjectType.builder()
            .setInnerType("c1", DataTypes.INTEGER).build();
        var ooType = ObjectType.builder()
            .setInnerType("o", oooType)
            .setInnerType("b1", DataTypes.INTEGER)
            .setInnerType("b2", DataTypes.INTEGER).build();
        var oType = ObjectType.builder()
            .setInnerType("o", ooType)
            .setInnerType("a1", DataTypes.INTEGER).build();

        assertThat(newTable.getReference(ColumnIdent.of("o", List.of("o", "o"))))
            .hasName("o['o']['o']")
            .hasType(oooType);
        assertThat(newTable.getReference(ColumnIdent.of("o", List.of("o"))))
            .hasName("o['o']")
            .hasType(ooType);
        assertThat(newTable.getReference(ColumnIdent.of("o")))
            .hasName("o")
            .hasType(oType);
    }

    @Test
    public void test_drop_column_fixes_inner_types_of_all_its_parents() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (o object as (o object as (o object as (c1 int), b1 int), a1 int))");
        DocTableInfo table = e.resolveTableInfo("tbl");
        ColumnIdent dropCol1 = ColumnIdent.of("o", List.of("o", "o"));
        ColumnIdent dropCol2 = ColumnIdent.of("o", List.of("o", "o", "c1"));
        ColumnIdent dropCol3 = ColumnIdent.of("o", List.of("o", "b1"));
        DocTableInfo newTable = table.dropColumns(
            List.of(
                new DropColumn(table.getReference(dropCol1), false),
                new DropColumn(table.getReference(dropCol2), false),
                new DropColumn(table.getReference(dropCol3), false)
            )
        );

        var ooType = ObjectType.builder().build();
        var oType = ObjectType.builder()
            .setInnerType("o", ooType)
            .setInnerType("a1", DataTypes.INTEGER).build();

        assertThat(newTable.getReference(dropCol1)).isNull();
        assertThat(newTable.getReference(dropCol2)).isNull();
        assertThat(newTable.getReference(dropCol3)).isNull();
        assertThat(newTable.getReference(ColumnIdent.of("o", List.of("o"))))
            .hasName("o['o']")
            .hasType(ooType);
        assertThat(newTable.getReference(ColumnIdent.of("o")))
            .hasName("o")
            .hasType(oType);
    }

    @Test
    public void test_rename_column_fixes_inner_types_of_all_its_parents() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (o object as (o object as (o object as (c1 int), b1 int), a1 int))");
        DocTableInfo table = e.resolveTableInfo("tbl");
        ColumnIdent ooo = ColumnIdent.of("o", List.of("o", "o"));
        ColumnIdent ooo2 = ColumnIdent.of("o", List.of("o", "o2"));
        table = table.renameColumn(table.getReference(ooo), ooo2);

        var ooo2Type = ObjectType.builder()
            .setInnerType("c1", DataTypes.INTEGER).build();
        var ooType = ObjectType.builder()
            .setInnerType("o2", ooo2Type)
            .setInnerType("b1", DataTypes.INTEGER).build();
        var oType = ObjectType.builder()
            .setInnerType("o", ooType)
            .setInnerType("a1", DataTypes.INTEGER).build();

        ColumnIdent oo = ColumnIdent.of("o", List.of("o"));
        ColumnIdent o = ColumnIdent.of("o");

        assertThat(table.getReference(ooo)).isNull();
        assertThat(table.getReference(ooo2))
            .hasName("o['o']['o2']")
            .hasType(ooo2Type);
        assertThat(table.getReference(oo)).hasName("o['o']")
            .hasType(ooType);
        assertThat(table.getReference(o)).hasName("o").hasType(oType);
    }

    /**
     * Tests a regression introduced by https://github.com/crate/crate/commit/111ffe166e523a4a5cd3278975772ce365112b64
     * where the number of shards of a partitioned table was not preserved when writing the table info to metadata.
     */
    @UseRandomizedSchema(random = false)
    @Test
    public void test_write_to_preserves_number_of_shards_of_partitions() throws Exception {
        var partitionIndexName = new PartitionName(new RelationName("doc", "tbl"), singletonList("1")).asIndexName();
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addPartitionedTable(
                """
                    create table tbl (
                        id int,
                        p int
                    ) clustered into 2 shards partitioned by (p) with (number_of_replicas=0)
                    """,
                partitionIndexName
            );

        ClusterState state = clusterService.state();
        Metadata metadata = state.metadata();

        // Change the number of shards of the partition table/template aka. ALTER TABLE tbl SET (number_of_shards=3)
        var oldTemplate = metadata.templates().get(PartitionName.templateName("doc", "tbl"));
        var newTemplate = new IndexTemplateMetadata.Builder(oldTemplate);
        newTemplate.settings(Settings.builder().put(oldTemplate.settings()).put("index.number_of_shards", 3));

        Metadata.Builder builder = new Metadata.Builder(metadata);
        builder.put(newTemplate);
        var newMetadata = builder.build();

        DocTableInfoFactory docTableInfoFactory = new DocTableInfoFactory(e.nodeCtx);
        DocTableInfo tbl = docTableInfoFactory.create(RelationName.fromIndexName("tbl"), newMetadata);

        var newBuilder = new Metadata.Builder(metadata);
        tbl.writeTo(newMetadata, newBuilder);

        var partitionIndex = newBuilder.build().index(partitionIndexName);
        assertThat(partitionIndex.getNumberOfShards()).isEqualTo(2);
    }

    @Test
    public void test_get_child_references() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("""
                create table tbl (
                    c1 int,
                    c2 text,
                    c3 array(int),
                    c4 object as (d1 int, d2 object as (e1 int, e2 int)),
                    c5 array(object as (d1 int, d2 object as (e1 int, e2 int, e3 int)))
                )
                """);

        DocTableInfo table = e.resolveTableInfo("tbl");

        assertThat(table.findParentReferenceMatching(
            DocReferences.toDocLookup(table.getReference("c5.d2.e1")),
            r -> r.valueType() instanceof ArrayType<?>)
        ).isEqualTo(table.getReference("c5"));

        assertThat(table.getLeafReferences(DocReferences.toDocLookup(table.getReference("c1"))))
            .hasSize(1)
            .containsExactly(table.getReference("c1"));

        assertThat(table.getLeafReferences(table.getReference("c4")))
            .hasSize(3);

        assertThat(table.getLeafReferences(table.getReference("c5")))
            .hasSize(4);

        assertThat(table.getLeafReferences(table.getReference("c1")))
            .hasSize(1);
    }
}

/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.InvalidColumnNameException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class AlterTableRenameColumnAnalyzerTest extends CrateDummyClusterServiceUnitTest {
    private SQLExecutor e;

    @Test
    public void test_rename_top_level_column() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (a int)");

        AnalyzedAlterTableRenameColumn analyzed = e.analyze("alter table t rename column a to b");
        assertThat(analyzed.refToRename()).isReference().hasName("a");
        assertThat(analyzed.newName()).isEqualTo(new ColumnIdent("b"));
        assertThat(analyzed.table()).isEqualTo(new RelationName("doc", "t"));
    }

    @Test
    public void test_rename_nested_column() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (o object as (o2 object as (o3 object)))");

        AnalyzedAlterTableRenameColumn analyzed = e.analyze("alter table t rename column o['o2']['o3'] to o['o2']['x']");
        assertThat(analyzed.refToRename()).isReference().hasColumnIdent(new ColumnIdent("o", List.of("o2", "o3")));
        assertThat(analyzed.newName()).isEqualTo(new ColumnIdent("o", List.of("o2", "x")));
        assertThat(analyzed.table()).isEqualTo(new RelationName("doc", "t"));
    }

    @Test
    public void test_rename_nested_column_record_subscripts() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (o object as (o2 object as (o3 object)))");

        AnalyzedAlterTableRenameColumn analyzed = e.analyze("alter table t rename column o['o2']['o3'] to o['o2']['x']");
        assertThat(analyzed.refToRename()).isReference().hasColumnIdent(new ColumnIdent("o", List.of("o2", "o3")));
        assertThat(analyzed.newName()).isEqualTo(new ColumnIdent("o", List.of("o2", "x")));
        assertThat(analyzed.table()).isEqualTo(new RelationName("doc", "t"));
    }

    @Test
    public void test_cannot_rename_nested_column_to_target_name_with_different_parent() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (o object as (o2 object as (o3 object)))");

        assertThatThrownBy(() -> e.analyze("alter table t rename column o['o2']['o3'] to o['x']['x']"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("When renaming sub-columns, parent names must be equal: o['o2'], o['x']");
    }

    @Test
    public void test_cannot_rename_nested_column_to_target_name_with_different_depths() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (o object as (o2 object as (o3 object)))");

        assertThatThrownBy(() -> e.analyze("alter table t rename column o['o2']['o3'] to o['o2']['o3']['y']"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot rename a column to a name that has different column level: o['o2']['o3'], o['o2']['o3']['y']");
    }

    @Test
    public void test_cannot_rename_unknown_column() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (o object)");

        assertThatThrownBy(() -> e.analyze("alter table t rename column o['unknown1'] to o['unknown2']"))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column o['unknown1'] unknown");
    }

    @Test
    public void test_cannot_rename_to_name_in_use() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (a int, b int)");

        assertThatThrownBy(() -> e.analyze("alter table t rename column a to b"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot rename column to a name that is in use");
    }

    @Test
    public void test_cannot_rename_column_from_single_partition() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS);

        assertThatThrownBy(() -> e.analyze("ALTER TABLE parted partition (date = 1395874800000) rename column name to newName"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Renaming a column from a single partition is not supported");
    }

    @Test
    public void test_renaming_column_from_old_table_is_not_allowed() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable(
                "create table t (a int)",
                Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_5_4_0).build()
            );

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t RENAME COLUMN a to b"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Renaming columns of a table created before version 5.5 is not supported");
    }

    @Test
    public void test_renaming_index_columns_to_subscript_expressions_is_not_allowed() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (a text, index b using fulltext (a))");

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t RENAME COLUMN b to o['b']"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot rename a column to a name that has different column level: b, o['b']");
    }

    @Test
    public void test_renaming_to_or_from_system_columns_is_not_allowed() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (a text)");
        assertThatThrownBy(() -> e.analyze("ALTER TABLE t RENAME COLUMN _doc to x"))
            .isExactlyInstanceOf(InvalidColumnNameException.class)
            .hasMessage("\"_doc\" conflicts with system column pattern");
        assertThatThrownBy(() -> e.analyze("ALTER TABLE t RENAME a to _doc"))
            .isExactlyInstanceOf(InvalidColumnNameException.class)
            .hasMessage("\"_doc\" conflicts with system column pattern");
    }
}

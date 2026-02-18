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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class AlterTableAlterColumnDefaultAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Test
    public void test_set_default_on_column() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (a int)");

        AnalyzedAlterTableAlterColumnDefault analyzed = e.analyze("alter table t alter column a set default 42");
        assertThat(analyzed.ref()).hasName("a");
        assertThat(analyzed.newDefault()).isNotNull();
        assertThat(analyzed.table()).isEqualTo(new RelationName("doc", "t"));
    }

    @Test
    public void test_drop_default_on_column() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (a int default 42)");

        AnalyzedAlterTableAlterColumnDefault analyzed = e.analyze("alter table t alter column a drop default");
        assertThat(analyzed.ref()).hasName("a");
        assertThat(analyzed.newDefault()).isNull();
        assertThat(analyzed.table()).isEqualTo(new RelationName("doc", "t"));
    }

    @Test
    public void test_set_default_without_column_keyword() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (a int)");

        AnalyzedAlterTableAlterColumnDefault analyzed = e.analyze("alter table t alter a set default 42");
        assertThat(analyzed.ref()).hasName("a");
        assertThat(analyzed.newDefault()).isNotNull();
    }

    @Test
    public void test_cannot_set_default_on_generated_column() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (a int, b int generated always as (a + 1))");

        assertThatThrownBy(() -> e.analyze("alter table t alter column b set default 5"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot SET DEFAULT on generated column");
    }

    @Test
    public void test_cannot_set_default_on_object_column() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (o object as (a int))");

        assertThatThrownBy(() -> e.analyze("alter table t alter column o set default {a=1}"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Default values are not allowed for object columns");
    }

    @Test
    public void test_cannot_set_default_with_type_mismatch() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (a int)");

        assertThatThrownBy(() -> e.analyze("alter table t alter column a set default 'hello'"))
            .hasMessageContaining("Cannot cast");
    }

    @Test
    public void test_cannot_set_default_from_single_partition() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS);

        assertThatThrownBy(() -> e.analyze("ALTER TABLE parted partition (date = 1395874800000) alter column name set default 'x'"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Altering a column default from a single partition is not supported");
    }

    @Test
    public void test_cannot_set_default_on_old_table() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable(
                "create table t (a int)",
                Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_5_4_0).build()
            );

        assertThatThrownBy(() -> e.analyze("ALTER TABLE t ALTER COLUMN a SET DEFAULT 42"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Altering the default of columns of a table created before version 5.5.0 is not supported");
    }

    @Test
    public void test_cannot_reference_columns_in_default_expression() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (a int, b int)");

        assertThatThrownBy(() -> e.analyze("alter table t alter column a set default b"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cannot reference columns in DEFAULT expression");
    }

    @Test
    public void test_set_default_on_nested_column() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (o object as (a int))");

        AnalyzedAlterTableAlterColumnDefault analyzed = e.analyze("alter table t alter column o['a'] set default 10");
        assertThat(analyzed.ref()).hasName("o['a']");
        assertThat(analyzed.newDefault()).isNotNull();
    }

    @Test
    public void test_set_default_on_primary_key_column() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (id int primary key, name text)");

        AnalyzedAlterTableAlterColumnDefault analyzed = e.analyze("alter table t alter column id set default 1");
        assertThat(analyzed.ref()).hasName("id");
        assertThat(analyzed.newDefault()).isNotNull();
    }

    @Test
    public void test_set_default_on_array_column() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (tags array(text))");

        AnalyzedAlterTableAlterColumnDefault analyzed = e.analyze("alter table t alter column tags set default ['a', 'b']");
        assertThat(analyzed.ref()).hasName("tags");
        assertThat(analyzed.newDefault()).isNotNull();
    }

    @Test
    public void test_set_default_on_partition_column() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t (a int, p text) partitioned by (p)");

        AnalyzedAlterTableAlterColumnDefault analyzed = e.analyze("alter table t alter column p set default 'default_partition'");
        assertThat(analyzed.ref()).hasName("p");
        assertThat(analyzed.newDefault()).isNotNull();
    }
}

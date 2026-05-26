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

package org.elasticsearch.snapshots;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.exceptions.IncompatibleSchemaForRestoreException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.StringType;

public class SnapshotSchemaValidatorTest {

    private static final Snapshot SNAP =
        new Snapshot("repo", new SnapshotId("snap1", "uuid-snap1"));

    private static final RelationName REL = new RelationName("doc", "t");

    @Test
    public void test_identical_schema_passes() {
        var snap = tableWith(col("a", (long) 1, true, DataTypes.INTEGER));
        var target = tableWith(col("a", (long) 1, true, DataTypes.INTEGER));
        assertThatCode(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .doesNotThrowAnyException();
    }

    @Test
    public void test_column_type_mismatch_throws() {
        var snap = tableWith(col("a", (long) 1, true, DataTypes.INTEGER));
        var target = tableWith(col("a", (long) 1, true, DataTypes.STRING));
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("cannot restore relation [doc.t]")
            .hasMessageContaining("column [a] type mismatch - snapshot: [integer], target: [text]");
    }

    @Test
    public void test_parameterized_type_mismatch_throws() {
        var snap = tableWith(col("a", (long) 1, true, StringType.of(10)));
        var target = tableWith(col("a", (long) 1, true, StringType.of(20)));
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("column [a]")
            .hasMessageContaining("type");
    }

    @Test
    public void test_oid_mismatch_throws() {
        var snap = tableWith(col("a", (long) 10, true, DataTypes.INTEGER));
        var target = tableWith(col("a", (long) 42, true, DataTypes.INTEGER));
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("column [a] OID mismatch - snapshot: [10], target: [42]");
    }

    @Test
    public void test_index_type_mismatch_throws() {
        var snap = tableWith(textColWithIndexType("a", 1, IndexType.PLAIN));
        var target = tableWith(textColWithIndexType("a", 1, IndexType.FULLTEXT));
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("column [a] indexType mismatch");
    }

    @Test
    public void test_doc_values_mismatch_throws() {
        var snap = tableWith(intColWithDocValues("a", 1, true));
        var target = tableWith(intColWithDocValues("a", 1, false));
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("column [a] doc-values mismatch");
    }

    @Test
    public void test_column_missing_in_target_throws() {
        var snap = tableWith(col("a", (long) 1, true, DataTypes.INTEGER), col("b", (long) 2, true, DataTypes.INTEGER));
        var target = tableWith(col("a", (long) 1, true, DataTypes.INTEGER));
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("column [b] is missing from the target");
    }

    @Test
    public void test_extra_nullable_column_in_target_passes() {
        var snap = tableWith(col("a", (long) 1, true, DataTypes.INTEGER));
        var target = tableWith(col("a", (long) 1, true, DataTypes.INTEGER), col("c", (long) 2, true, DataTypes.INTEGER));
        assertThatCode(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .doesNotThrowAnyException();
    }

    @Test
    public void test_extra_not_null_column_in_target_throws() {
        var snap = tableWith(col("a", (long) 1, true, DataTypes.INTEGER));
        var target = tableWith(col("a", (long) 1, true, DataTypes.INTEGER), col("c", (long) 2, false, DataTypes.INTEGER));
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("column [c] is NOT NULL in the target but not present in the snapshot");
    }

    @Test
    public void test_target_stricter_nullability_throws() {
        var snap = tableWith(col("a", (long) 1, true, DataTypes.INTEGER));     // nullable
        var target = tableWith(col("a", (long) 1, false, DataTypes.INTEGER));  // NOT NULL
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("column [a] nullability mismatch - snapshot: nullable, target: NOT NULL");
    }

    @Test
    public void test_target_looser_nullability_passes() {
        var snap = tableWith(col("a", (long) 1, false, DataTypes.INTEGER));    // NOT NULL
        var target = tableWith(col("a", (long) 1, true, DataTypes.INTEGER));   // nullable
        assertThatCode(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .doesNotThrowAnyException();
    }

    @Test
    public void test_primary_key_columns_mismatch_throws() {
        var snap = table(List.of(col("a", (long) 1, false, DataTypes.INTEGER), col("b", (long) 2, false, DataTypes.INTEGER)),
                         null, List.of(ColumnIdent.of("a"), ColumnIdent.of("b")), List.of());
        var target = table(List.of(col("a", (long) 1, false, DataTypes.INTEGER), col("b", (long) 2, false, DataTypes.INTEGER)),
                           null, List.of(ColumnIdent.of("a")), List.of());
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("PRIMARY KEY columns differ");
    }

    @Test
    public void test_pk_constraint_name_mismatch_throws() {
        var snap = table(List.of(col("a", (long) 1, false, DataTypes.INTEGER)),
                         "pk_a", List.of(ColumnIdent.of("a")), List.of());
        var target = table(List.of(col("a", (long) 1, false, DataTypes.INTEGER)),
                           "pk_other", List.of(ColumnIdent.of("a")), List.of());
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("PRIMARY KEY constraint name differs");
    }

    @Test
    public void test_partitioned_by_mismatch_throws() {
        var snap = table(List.of(col("a", (long) 1, true, DataTypes.INTEGER), col("p", (long) 2, true, DataTypes.INTEGER)),
                         null, List.of(), List.of(ColumnIdent.of("p")));
        var target = table(List.of(col("a", (long) 1, true, DataTypes.INTEGER), col("p", (long) 2, true, DataTypes.INTEGER)),
                           null, List.of(), List.of());
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("PARTITIONED BY columns differ");
    }

    @Test
    public void test_clustered_by_mismatch_throws() {
        // Both sides have explicit (non-null) CLUSTERED BY but on different columns.
        var snap = table(List.of(col("a", (long) 1, true, DataTypes.INTEGER), col("b", (long) 2, true, DataTypes.INTEGER)),
                         ColumnIdent.of("a"), null, List.of(), List.of());
        var target = table(List.of(col("a", (long) 1, true, DataTypes.INTEGER), col("b", (long) 2, true, DataTypes.INTEGER)),
                           ColumnIdent.of("b"), null, List.of(), List.of());
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("CLUSTERED BY column differs");
    }

    @Test
    public void test_clustered_by_null_on_one_side_does_not_throw() {
        // null routingColumn is the "implicit default" derived from PK at read time.
        // Treated as equal to any explicit routingColumn on the other side until
        // CrateDB unifies the two representations (see TODO in validator).
        RelationMetadata.Table snap = table(
            List.of(col("a", (long) 1, true, DataTypes.INTEGER)),
            null,
            null,
            List.of(ColumnIdent.of("a")),
            List.of()
        );
        RelationMetadata.Table target = table(
            List.of(col("a", (long) 1, true, DataTypes.INTEGER)),
            ColumnIdent.of("a"),
            null,
            List.of(ColumnIdent.of("a")),
            List.of()
        );
        assertThatCode(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .doesNotThrowAnyException();
    }

    @Test
    public void test_rename_on_restore_error_uses_target_name() {
        // RESTORE SNAPSHOT ... TABLE foo AS bar - the source relation in the
        // snapshot is foo but the user is restoring it into bar. Error
        // messages must name the target (bar), not the source (foo).
        var snapRel = new RelationName("doc", "foo");
        var targetRel = new RelationName("doc", "bar");
        var snap = new RelationMetadata.Table(
            0,
            snapRel,
            List.of(col("a", (long) 1, true, DataTypes.INTEGER)),
            baseSettings().build(),
            null,
            ColumnPolicy.DYNAMIC,
            null,
            Map.of(),
            List.of(),
            List.of(),
            State.OPEN,
            List.of(UUIDs.randomBase64UUID()),
            0L);
        var target = new RelationMetadata.Table(
            0,
            targetRel,
            List.of(col("a", (long) 1, true, DataTypes.STRING)),
            baseSettings().build(),
            null,
            ColumnPolicy.DYNAMIC,
            null,
            Map.of(),
            List.of(),
            List.of(),
            State.OPEN,
            List.of(UUIDs.randomBase64UUID()),
            0L);
        assertThatThrownBy(() -> SnapshotSchemaValidator.validate(SNAP, targetRel, snap, target))
            .isInstanceOf(IncompatibleSchemaForRestoreException.class)
            .hasMessageContaining("cannot restore relation [doc.bar]")
            .hasMessageNotContaining("doc.foo");
    }

    @Test
    public void test_settings_difference_does_not_throw() {
        // Same schema; different number_of_replicas in the underlying Settings.
        // The validator does not check Settings - that's filtered by Change B.
        var snap = tableWith(col("a", (long) 1, true, DataTypes.INTEGER));
        var targetSettings = baseSettings()
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 3)
            .build();
        var target = new RelationMetadata.Table(
            0,
            REL,
            List.of(col("a", (long) 1, true, DataTypes.INTEGER)),
            targetSettings,
            null,
            ColumnPolicy.DYNAMIC,
            null,
            Map.of(),
            List.of(),
            List.of(),
            State.OPEN,
            List.of(UUIDs.randomBase64UUID()),
            0L);
        assertThatCode(() -> SnapshotSchemaValidator.validate(SNAP, REL, snap, target))
            .doesNotThrowAnyException();
    }

    // -- fixture helpers --

    private static Reference col(String name, long oid, boolean nullable, DataType<?> type) {
        return new SimpleReference(
            REL,
            ColumnIdent.of(name),
            RowGranularity.DOC,
            type,
            IndexType.PLAIN,
            nullable,
            true,
            (int) oid,
            oid,
            false,
            null);
    }

    private static Reference textColWithIndexType(String name, long oid, IndexType indexType) {
        return new SimpleReference(
            REL,
            ColumnIdent.of(name),
            RowGranularity.DOC,
            DataTypes.STRING,
            indexType,
            true,
            true,
            (int) oid,
            oid,
            false,
            null);
    }

    private static Reference intColWithDocValues(String name, long oid, boolean hasDocValues) {
        return new SimpleReference(
            REL,
            ColumnIdent.of(name),
            RowGranularity.DOC,
            DataTypes.INTEGER,
            IndexType.PLAIN,
            true,
            hasDocValues,
            (int) oid,
            oid,
            false,
            null);
    }

    private static Settings.Builder baseSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
    }

    private static RelationMetadata.Table tableWith(Reference... cols) {
        return table(List.of(cols), null, List.of(), List.of());
    }

    private static RelationMetadata.Table table(List<Reference> cols,
                                                String pkConstraintName,
                                                List<ColumnIdent> primaryKeys,
                                                List<ColumnIdent> partitionedBy) {
        return table(cols, null, pkConstraintName, primaryKeys, partitionedBy);
    }

    private static RelationMetadata.Table table(List<Reference> cols,
                                                ColumnIdent routingColumn,
                                                String pkConstraintName,
                                                List<ColumnIdent> primaryKeys,
                                                List<ColumnIdent> partitionedBy) {
        // Honor the record's invariant: non-partitioned tables must have exactly one indexUUID.
        List<String> indexUUIDs = partitionedBy.isEmpty()
            ? List.of(UUIDs.randomBase64UUID())
            : List.of();
        return new RelationMetadata.Table(
            0,
            REL,
            cols,
            baseSettings().build(),
            routingColumn,
            ColumnPolicy.DYNAMIC,
            pkConstraintName,
            Map.of(),
            primaryKeys,
            partitionedBy,
            State.OPEN,
            indexUUIDs,
            0L);
    }
}

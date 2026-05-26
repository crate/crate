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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.cluster.metadata.RelationMetadata;

import io.crate.exceptions.IncompatibleSchemaForRestoreException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

/**
 * Validates that a snapshot's table schema is compatible with the existing
 * target table's schema.
 *
 * <p>Strict equality modeled on PostgreSQL {@code ALTER TABLE ... ATTACH
 * PARTITION}: snapshot's Lucene segment files are copied byte-for-byte into
 * the target, so anything that affects on-disk layout must match.
 *
 * <p>Rules enforced:
 * <ul>
 *   <li>Per shared column: same {@link ColumnIdent}, same OID, same
 *       {@link io.crate.types.DataType} (id + parameters), same
 *       {@code indexType}, same {@code hasDocValues}, target nullability
 *       &supseteq; snapshot nullability.</li>
 *   <li>Snapshot-side columns missing from target &rarr; reject.</li>
 *   <li>Extra target column: allowed if nullable (preserved via merge in
 *       {@code RestoreService.restoreRelation}), rejected if NOT NULL.</li>
 *   <li>Relation-level: same primary key columns, same {@code pkConstraintName},
 *       same partitioned-by columns, same CLUSTERED BY column ({@code null}
 *       is treated as equal to the first primary-key column).</li>
 * </ul>
 *
 * <p>Fails on the first mismatch encountered.
 */
public final class SnapshotSchemaValidator {

    private SnapshotSchemaValidator() {
    }

    public static void validate(Snapshot snapshot,
                                RelationName targetName,
                                RelationMetadata.Table snapshotTable,
                                RelationMetadata.Table targetTable) {
        validateColumns(snapshot, targetName, snapshotTable, targetTable);
        validateConstraints(snapshot, targetName, snapshotTable, targetTable);
    }

    private static void validateColumns(Snapshot snapshot,
                                        RelationName targetName,
                                        RelationMetadata.Table snapshotTable,
                                        RelationMetadata.Table targetTable) {
        Map<ColumnIdent, Reference> snapshotCols = byColumn(snapshotTable);
        Map<ColumnIdent, Reference> targetCols = byColumn(targetTable);

        for (Map.Entry<ColumnIdent, Reference> e : snapshotCols.entrySet()) {
            ColumnIdent column = e.getKey();
            Reference snapshotRef = e.getValue();
            Reference targetRef = targetCols.get(column);
            if (targetRef == null) {
                throw mismatch(snapshot, targetName,
                    "column [" + column.fqn() + "] is missing from the target");
            }
            checkColumn(snapshot, targetName, snapshotRef, targetRef);
        }
        for (Map.Entry<ColumnIdent, Reference> e : targetCols.entrySet()) {
            ColumnIdent column = e.getKey();
            Reference targetRef = e.getValue();
            if (!snapshotCols.containsKey(column) && !targetRef.isNullable()) {
                throw mismatch(snapshot, targetName,
                    "column [" + column.fqn() + "] is NOT NULL in the target but not present in the snapshot");
            }
        }
    }

    private static void checkColumn(Snapshot snapshot,
                                    RelationName targetName,
                                    Reference snapshotRef,
                                    Reference targetRef) {
        String col = snapshotRef.column().fqn();
        if (!snapshotRef.valueType().equals(targetRef.valueType())) {
            throw mismatch(snapshot, targetName,
                "column [" + col + "] type mismatch - snapshot: ["
                    + snapshotRef.valueType().getName() + "], target: ["
                    + targetRef.valueType().getName() + "]");
        }
        if (snapshotRef.oid() != targetRef.oid()) {
            throw mismatch(snapshot, targetName,
                "column [" + col + "] OID mismatch - snapshot: ["
                    + snapshotRef.oid() + "], target: ["
                    + targetRef.oid() + "]");
        }
        if (snapshotRef.indexType() != targetRef.indexType()) {
            throw mismatch(snapshot, targetName,
                "column [" + col + "] indexType mismatch - snapshot: ["
                    + snapshotRef.indexType() + "], target: ["
                    + targetRef.indexType() + "]");
        }
        if (snapshotRef.hasDocValues() != targetRef.hasDocValues()) {
            throw mismatch(snapshot, targetName,
                "column [" + col + "] doc-values mismatch - snapshot: "
                    + snapshotRef.hasDocValues() + ", target: "
                    + targetRef.hasDocValues());
        }
        if (snapshotRef.isNullable() && !targetRef.isNullable()) {
            throw mismatch(snapshot, targetName,
                "column [" + col + "] nullability mismatch - snapshot: nullable, target: NOT NULL");
        }
    }

    private static void validateConstraints(Snapshot snapshot,
                                            RelationName targetName,
                                            RelationMetadata.Table snapshotTable,
                                            RelationMetadata.Table targetTable) {
        if (!snapshotTable.primaryKeys().equals(targetTable.primaryKeys())) {
            throw mismatch(snapshot, targetName, "PRIMARY KEY columns differ");
        }
        if (!Objects.equals(snapshotTable.pkConstraintName(), targetTable.pkConstraintName())) {
            throw mismatch(snapshot, targetName, "PRIMARY KEY constraint name differs");
        }
        if (!snapshotTable.partitionedBy().equals(targetTable.partitionedBy())) {
            throw mismatch(snapshot, targetName, "PARTITIONED BY columns differ");
        }
        if (!snapshotTable.routingColumnOrDefault().equals(targetTable.routingColumnOrDefault())) {
            throw mismatch(snapshot, targetName, "CLUSTERED BY column differs");
        }
    }

    private static Map<ColumnIdent, Reference> byColumn(RelationMetadata.Table table) {
        // Exclude dropped columns: They don't have to match - users can't read/access them.
        // LinkedHashMap to preserve order for deterministic error messages
        Map<ColumnIdent, Reference> map = LinkedHashMap.newLinkedHashMap(table.columns().size());
        for (Reference ref : table.columns()) {
            if (!ref.isDropped()) {
                map.put(ref.column(), ref);
            }
        }
        return map;
    }

    private static IncompatibleSchemaForRestoreException mismatch(Snapshot snapshot,
                                                                  RelationName targetName,
                                                                  String detail) {
        return new IncompatibleSchemaForRestoreException(snapshot, targetName, detail);
    }
}

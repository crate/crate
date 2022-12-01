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

package io.crate.metadata;

import io.crate.blob.v2.BlobIndex;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.execution.ddl.tables.AlterTableOperation;
import io.crate.metadata.blob.BlobSchemaInfo;

import javax.annotation.Nullable;
import java.util.List;

/**
 * 1) Class which unpacks and holds the different entities of a CrateDB index name.
 * 2) Static methods to check index names or generate them for RelationName or PartitionName
 */
public class IndexParts {

    private static final String PARTITIONED_KEY_WORD = "partitioned";
    @VisibleForTesting
    static final String PARTITIONED_TABLE_PART = "." + PARTITIONED_KEY_WORD + ".";

    public static final List<String> DANGLING_INDICES_PREFIX_PATTERNS = List.of(
        AlterTableOperation.RESIZE_PREFIX + "*"
    );

    private final String schema;
    private final String table;
    private final String partitionIdent;

    public IndexParts(String indexName) {
        this(indexName, Schemas.DOC_SCHEMA_NAME);
    }

    public IndexParts(String indexName, String defaultSchema) {
        if (BlobIndex.isBlobIndex(indexName)) {
            schema = BlobSchemaInfo.NAME;
            table = BlobIndex.stripPrefix(indexName);
            partitionIdent = null;
        } else {
            // Index names are only allowed to contain '.' as separators
            List<String> parts = List.of(indexName.split("\\.", 6));
            switch (parts.size()) {
                case 1:
                    // "table_name"
                    schema = defaultSchema;
                    table = indexName;
                    partitionIdent = null;
                    break;
                case 2:
                    // "schema"."table_name"
                    schema = parts.get(0);
                    table = parts.get(1);
                    partitionIdent = null;
                    break;
                case 4:
                    // ""."partitioned"."table_name". ["ident"]
                    assertEmpty(parts.get(0));
                    schema = defaultSchema;
                    assertPartitionPrefix(parts.get(1));
                    table = parts.get(2);
                    partitionIdent = parts.get(3);
                    break;
                case 5:
                    // "schema".""."partitioned"."table_name". ["ident"]
                    schema = parts.get(0);
                    assertEmpty(parts.get(1));
                    assertPartitionPrefix(parts.get(2));
                    table = parts.get(3);
                    partitionIdent = parts.get(4);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid index name: " + indexName);
            }
        }
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getPartitionIdent() {
        return isPartitioned() ? partitionIdent : "";
    }

    public boolean isPartitioned() {
        return partitionIdent != null;
    }

    public RelationName toRelationName() {
        return new RelationName(schema, table);
    }

    public String toFullyQualifiedName() {
        return schema + "." + table;
    }

    public boolean matchesSchema(String schema) {
        return this.schema.equals(schema);
    }

    /////////////////////////
    // Static utility methods
    /////////////////////////

    public static String toIndexName(RelationName relationName, String partitionIdent) {
        return toIndexName(relationName.schema(), relationName.name(), partitionIdent);
    }

    public static String toIndexName(PartitionName partitionName) {
        RelationName relationName = partitionName.relationName();
        return toIndexName(relationName.schema(), relationName.name(), partitionName.ident());
    }

    /**
     * Encodes the given parts to a CrateDB index name.
     */
    public static String toIndexName(String schema, String table, @Nullable String partitionIdent) {
        StringBuilder stringBuilder = new StringBuilder();
        final boolean isPartitioned = partitionIdent != null;
        if (!schema.equals(Schemas.DOC_SCHEMA_NAME)) {
            stringBuilder.append(schema).append(".");
        }
        if (isPartitioned) {
            stringBuilder.append(PARTITIONED_TABLE_PART);
        }
        stringBuilder.append(table);
        if (isPartitioned) {
            stringBuilder.append(".").append(partitionIdent);
        }
        return stringBuilder.toString();
    }

    /**
     * Checks whether the index/template name belongs to a partitioned table.
     *
     * A partition index name looks like on of these:
     *
     * .partitioned.table.ident
     * schema..partitioned.table.ident
     * schema..partitioned.table.
     *
     * @param templateOrIndex The index name to check
     * @return True if the index/template name denotes a partitioned table
     */
    public static boolean isPartitioned(String templateOrIndex) {
        int idx1 = templateOrIndex.indexOf('.');
        if (idx1 == -1) {
            return false;
        }
        int idx2 = templateOrIndex.indexOf(PARTITIONED_TABLE_PART, idx1);
        if (idx2 == -1) {
            return false;
        }
        int diff = idx2 - idx1;
        return ((diff == 0 && idx1 == 0) || diff == 1) && idx2 + PARTITIONED_TABLE_PART.length() < templateOrIndex.length();
    }

    public static boolean isDangling(String indexName) {
        return indexName.startsWith(".") &&
               !indexName.startsWith(PARTITIONED_TABLE_PART) &&
               !BlobIndex.isBlobIndex(indexName);
    }

    private static void assertPartitionPrefix(String prefix) {
        if (!PARTITIONED_KEY_WORD.equals(prefix)) {
            throw new IllegalArgumentException("Invalid partition prefix: " + prefix);
        }
    }

    private static void assertEmpty(String prefix) {
        if (!"".equals(prefix)) {
            throw new IllegalArgumentException("Invalid index name: " + prefix);
        }
    }
}

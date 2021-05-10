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

package io.crate.metadata.table;

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import io.crate.common.collections.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public enum Operation {
    READ("READ"),
    UPDATE("UPDATE"),
    INSERT("INSERT"),
    DELETE("DELETE"),
    DROP("DROP"),
    ALTER("ALTER"),
    ALTER_BLOCKS("ALTER"),
    ALTER_OPEN_CLOSE("ALTER OPEN/CLOSE"),
    ALTER_TABLE_RENAME("ALTER RENAME"),
    ALTER_REROUTE("ALTER REROUTE"),
    REFRESH("REFRESH"),
    SHOW_CREATE("SHOW CREATE"),
    OPTIMIZE("OPTIMIZE"),
    COPY_TO("COPY TO"),
    RESTORE_SNAPSHOT("RESTORE SNAPSHOT"),
    CREATE_SNAPSHOT("CREATE SNAPSHOT"),;

    public static final EnumSet<Operation> ALL = EnumSet.allOf(Operation.class);
    public static final EnumSet<Operation> SYS_READ_ONLY = EnumSet.of(READ);
    public static final EnumSet<Operation> READ_ONLY = EnumSet.of(READ, ALTER_BLOCKS);
    public static final EnumSet<Operation> CLOSED_OPERATIONS = EnumSet.of(ALTER_OPEN_CLOSE, ALTER_TABLE_RENAME);
    public static final EnumSet<Operation> BLOB_OPERATIONS = EnumSet.of(READ, OPTIMIZE, ALTER, ALTER_REROUTE, DROP);
    public static final EnumSet<Operation> READ_DISABLED_OPERATIONS = EnumSet.of(UPDATE, INSERT, DELETE, DROP, ALTER,
        ALTER_OPEN_CLOSE, ALTER_REROUTE, ALTER_BLOCKS, REFRESH, OPTIMIZE);
    public static final EnumSet<Operation> WRITE_DISABLED_OPERATIONS = EnumSet.of(READ, ALTER, ALTER_OPEN_CLOSE,
        ALTER_BLOCKS, ALTER_REROUTE, SHOW_CREATE, REFRESH, OPTIMIZE, COPY_TO, CREATE_SNAPSHOT);
    public static final EnumSet<Operation> METADATA_DISABLED_OPERATIONS = EnumSet.of(READ, UPDATE, INSERT, DELETE,
        ALTER_BLOCKS, ALTER_OPEN_CLOSE, ALTER_REROUTE, REFRESH, SHOW_CREATE, OPTIMIZE);

    private final String representation;

    Operation(String representation) {
        this.representation = representation;
    }

    private static final Map<String, EnumSet<Operation>> BLOCK_SETTING_TO_OPERATIONS_MAP =
        MapBuilder.<String, EnumSet<Operation>>newMapBuilder()
            .put(IndexMetadata.SETTING_READ_ONLY, READ_ONLY)
            .put(IndexMetadata.SETTING_BLOCKS_READ, READ_DISABLED_OPERATIONS)
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, WRITE_DISABLED_OPERATIONS)
            .put(IndexMetadata.SETTING_BLOCKS_METADATA, METADATA_DISABLED_OPERATIONS)
            .map();

    public static EnumSet<Operation> buildFromIndexSettingsAndState(Settings settings, IndexMetadata.State state) {
        if (state == IndexMetadata.State.CLOSE) {
            return CLOSED_OPERATIONS;
        }
        Set<Operation> operations = ALL;
        for (Map.Entry<String, EnumSet<Operation>> entry : BLOCK_SETTING_TO_OPERATIONS_MAP.entrySet()) {
            if (!settings.getAsBoolean(entry.getKey(), false)) {
                continue;
            }
            operations = Sets.intersection(entry.getValue(), operations);
        }
        return EnumSet.copyOf(operations);
    }

    public static void blockedRaiseException(TableInfo tableInfo, Operation operation) {
        if (!tableInfo.supportedOperations().contains(operation)) {
            String exceptionMessage;
            // If the only supported operation is open/close, then the table must be closed.
            if (tableInfo.supportedOperations().equals(CLOSED_OPERATIONS)) {
                exceptionMessage = "The relation \"%s\" doesn't support or allow %s operations, as it is currently " +
                                   "closed.";
            } else if (tableInfo.supportedOperations().equals(SYS_READ_ONLY) ||
                       tableInfo.supportedOperations().equals(READ_ONLY)) {
                exceptionMessage = "The relation \"%s\" doesn't support or allow %s operations, as it is read-only.";
            } else {
                exceptionMessage = "The relation \"%s\" doesn't support or allow %s operations.";
            }
            throw new OperationOnInaccessibleRelationException(tableInfo.ident(), String.format(Locale.ENGLISH,
                exceptionMessage, tableInfo.ident().fqn(), operation));
        }
    }

    @Override
    public String toString() {
        return representation;
    }
}

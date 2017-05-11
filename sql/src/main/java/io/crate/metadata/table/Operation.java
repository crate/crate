/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.table;

import com.google.common.collect.Sets;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public enum Operation {
    READ,
    UPDATE,
    INSERT,
    DELETE,
    DROP,
    ALTER,
    ALTER_OPEN_CLOSE;

    public static final EnumSet<Operation> ALL = EnumSet.allOf(Operation.class);
    public static final EnumSet<Operation> READ_ONLY = EnumSet.of(Operation.READ);

    private static final Map<String, EnumSet<Operation>> BLOCK_SETTING_TO_OPERATIONS_MAP =
        MapBuilder.<String, EnumSet<Operation>>newMapBuilder()
            .put(IndexMetaData.SETTING_READ_ONLY, READ_ONLY)
            .put(IndexMetaData.SETTING_BLOCKS_READ, EnumSet.of(UPDATE, INSERT, DELETE, DROP, ALTER, ALTER_OPEN_CLOSE))
            .put(IndexMetaData.SETTING_BLOCKS_WRITE, EnumSet.of(READ, ALTER, ALTER_OPEN_CLOSE))
            .put(IndexMetaData.SETTING_BLOCKS_METADATA, EnumSet.of(READ, INSERT, UPDATE, DELETE, ALTER_OPEN_CLOSE))
            .map();

    public static EnumSet<Operation> buildFromIndexSettingsAndState(Settings settings, IndexMetaData.State state) {
        if (state == IndexMetaData.State.CLOSE) {
            return EnumSet.of(ALTER_OPEN_CLOSE);
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
            if (tableInfo.supportedOperations().equals(EnumSet.of(Operation.ALTER_OPEN_CLOSE))) {
                exceptionMessage = "The relation \"%s\" doesn't support or allow %s operations, as it is currently closed.";
            } else {
                exceptionMessage = "The relation \"%s\" doesn't support or allow %s operations.";
            }
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                exceptionMessage, tableInfo.ident().fqn(), operation));
        }
    }

    public static boolean isReadOnly(TableInfo tableInfo) {
        return tableInfo.supportedOperations().equals(READ_ONLY);
    }
}

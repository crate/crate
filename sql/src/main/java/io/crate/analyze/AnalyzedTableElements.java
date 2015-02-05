/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.*;

public class AnalyzedTableElements {

    List<AnalyzedColumnDefinition> partitionedByColumns = new ArrayList<>();
    List<AnalyzedColumnDefinition> columns = new ArrayList<>();
    Set<ColumnIdent> columnIdents = new HashSet<>();
    Map<ColumnIdent, String> columnTypes = new HashMap<>();
    List<String> primaryKeys;
    List<List<String>> partitionedBy;

    /**
     * additional primary keys that are not inline with a column definition
     */
    private List<String> additionalPrimaryKeys = new ArrayList<>();
    private Map<String, Set<String>> copyToMap = new HashMap<>();


    public Map<String, Object> toMapping() {
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> meta = new HashMap<>();
        Map<String, Object> properties = new HashMap<>(columns.size());

        Map<String, Object> indicesMap = new HashMap<>();
        for (AnalyzedColumnDefinition column : columns) {
            properties.put(column.name(), column.toMapping());
            if (column.isIndex()) {
                indicesMap.put(column.name(), column.toMetaIndicesMapping());
            }
        }

        if (!partitionedByColumns.isEmpty()) {
            meta.put("partitioned_by", partitionedBy());
        }
        if (!indicesMap.isEmpty()) {
            meta.put("indices", indicesMap);
        }
        if (!primaryKeys().isEmpty()) {
            meta.put("primary_keys", primaryKeys());
        }
        mapping.put("_meta", meta);
        mapping.put("properties", properties);
        mapping.put("_all", ImmutableMap.of("enabled", false));

        return mapping;
    }

    public List<List<String>> partitionedBy() {
        if (partitionedBy == null) {
            partitionedBy = new ArrayList<>(partitionedByColumns.size());
            for (AnalyzedColumnDefinition partitionedByColumn : partitionedByColumns) {
                partitionedBy.add(ImmutableList.of(
                        partitionedByColumn.ident().fqn(),
                        partitionedByColumn.dataType())
                );
            }
        }

        return partitionedBy;
    }

    private void expandColumnIdents() {
        for (AnalyzedColumnDefinition column : columns) {
            expandColumn(column);
        }
    }

    private void expandColumn(AnalyzedColumnDefinition column) {
        if (column.isIndex()) {
            columnIdents.remove(column.ident());
            return;
        }

        columnIdents.add(column.ident());
        columnTypes.put(column.ident(), column.dataType());
        for (AnalyzedColumnDefinition child : column.children()) {
            expandColumn(child);
        }
    }

    public List<String> primaryKeys() {
        if (primaryKeys == null) {
            primaryKeys = new ArrayList<>();
            for (AnalyzedColumnDefinition column : columns) {
                if (column.isPrimaryKey()) {
                    primaryKeys.add(column.ident().fqn());
                }
            }
            primaryKeys.addAll(additionalPrimaryKeys);
        }
        return primaryKeys;
    }

    public void addPrimaryKey(String fqColumnName) {
        additionalPrimaryKeys.add(fqColumnName);
    }

    public void add(AnalyzedColumnDefinition analyzedColumnDefinition) {
        if (columnIdents.contains(analyzedColumnDefinition.ident())) {
            throw new IllegalArgumentException(String.format(
                    "column \"%s\" specified more than once", analyzedColumnDefinition.ident().sqlFqn()));
        }
        columnIdents.add(analyzedColumnDefinition.ident());
        columns.add(analyzedColumnDefinition);
        columnTypes.put(analyzedColumnDefinition.ident(), analyzedColumnDefinition.dataType());
    }

    public Settings settings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        for (AnalyzedColumnDefinition column : columns) {
            builder.put(column.analyzerSettings());
        }
        return builder.build();
    }

    public void finalizeAndValidate() {
        expandColumnIdents();
        for (AnalyzedColumnDefinition column : columns) {
            column.validate();
            addCopyToInfo(column);
        }
        validateIndexDefinitions();
        validatePrimaryKeys();
    }

    private void addCopyToInfo(AnalyzedColumnDefinition column) {
        if (!column.isIndex()) {
            Set<String> targets = copyToMap.get(column.ident().fqn());
            if (targets != null) {
                column.addCopyTo(targets);
            }
        }
        for (AnalyzedColumnDefinition child : column.children()) {
            addCopyToInfo(child);
        }
    }

    private void validatePrimaryKeys() {
        for (String additionalPrimaryKey : additionalPrimaryKeys) {
            ColumnIdent columnIdent = ColumnIdent.fromPath(additionalPrimaryKey);
            if (!columnIdents.contains(columnIdent)) {
                throw new ColumnUnknownException(columnIdent.sqlFqn());
            }
        }
    }

    private void validateIndexDefinitions() {
        for (Map.Entry<String, Set<String>> entry : copyToMap.entrySet()) {
            ColumnIdent columnIdent = ColumnIdent.fromPath(entry.getKey());
            if (!columnIdents.contains(columnIdent)) {
                throw new ColumnUnknownException(columnIdent.sqlFqn());
            }
            if (!columnTypes.get(columnIdent).equalsIgnoreCase("string")) {
                throw new IllegalArgumentException("INDEX definition only support 'string' typed source columns");
            }
        }
    }

    public void addCopyTo(String sourceColumn, String targetIndex) {
         Set<String> targetColumns = copyToMap.get(sourceColumn);
         if (targetColumns == null) {
             targetColumns = new HashSet<>();
             copyToMap.put(sourceColumn, targetColumns);
         }
         targetColumns.add(targetIndex);
    }

    public Set<ColumnIdent> columnIdents() {
        return columnIdents;
    }

    @Nullable
    private AnalyzedColumnDefinition columnDefinitionByIdent(ColumnIdent ident) {
        AnalyzedColumnDefinition result = null;
        ColumnIdent root = ident.getRoot();
        for (AnalyzedColumnDefinition column : columns) {
            if (column.ident().equals(root)) {
                result = column;
                break;
            }
        }
        if (result == null) {
            return null;
        }

        if (result.ident().equals(ident)) {
            return result;
        }

        return findInChildren(result, ident);
    }

    private AnalyzedColumnDefinition findInChildren(AnalyzedColumnDefinition column,
                                                    ColumnIdent ident) {
        AnalyzedColumnDefinition result = null;
        for (AnalyzedColumnDefinition child : column.children()) {
            if (child.ident().equals(ident)) {
                result = child;
                break;
            }
            AnalyzedColumnDefinition inChildren = findInChildren(child, ident);
            if (inChildren != null) {
                return inChildren;
            }
        }
        return result;
    }

    public void changeToPartitionedByColumn(ColumnIdent partitionedByIdent, boolean skipIfNotFound) {
        Preconditions.checkArgument(!partitionedByIdent.name().startsWith("_"),
                "Cannot use system columns in PARTITIONED BY clause");

        // need to call primaryKeys() before the partition column is removed from the columns list
        if (!primaryKeys().isEmpty() && !primaryKeys().contains(partitionedByIdent.fqn())) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Cannot use non primary key column '%s' in PARTITIONED BY clause if primary key is set on table",
                    partitionedByIdent.sqlFqn()));
        }

        AnalyzedColumnDefinition columnDefinition = columnDefinitionByIdent(partitionedByIdent);
        if (columnDefinition == null) {
            if (skipIfNotFound) {
                return;
            }
            throw new ColumnUnknownException(partitionedByIdent.sqlFqn());
        }
        if (columnDefinition.dataType().equals("object")) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Cannot use object column '%s' in PARTITIONED BY clause",
                    columnDefinition.ident().sqlFqn()));
        }
        if (columnDefinition.isArrayOrInArray()) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Cannot use array column '%s' in PARTITIONED BY clause", columnDefinition.ident().sqlFqn()));


        }if (columnDefinition.index().equals("analyzed")) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Cannot use column '%s' with fulltext index in PARTITIONED BY clause",
                    columnDefinition.ident().sqlFqn()));
        }
        columnIdents.remove(columnDefinition.ident());
        columnDefinition.index(ReferenceInfo.IndexType.NO.toString());
        partitionedByColumns.add(columnDefinition);
    }

    public List<AnalyzedColumnDefinition> columns() {
        return columns;
    }
}

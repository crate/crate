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

package io.crate.analyze;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.common.collections.Lists;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.types.DataTypes;

public record BoundCreateTable(
        RelationName tableName,
        @Nullable
        String pkConstraintName,
        boolean ifNotExists,
        /**
         * In order of definition
         */
        Map<ColumnIdent, Reference> columns,
        TableParameter tableParameter,
        List<Reference> primaryKeys,
        /**
         * By constraint name; In order of definition
         **/
        Map<String, AnalyzedCheck> checks,
        ColumnIdent routingColumn,
        List<Symbol> partitionedByColumns) {

    public boolean isPartitioned() {
        return !partitionedByColumns.isEmpty();
    }

    @Nullable
    public String templateName() {
        return partitionedByColumns.isEmpty() ? null : PartitionName.templateName(tableName.schema(), tableName.name());
    }

    public String templatePrefix() {
        return partitionedByColumns.isEmpty() ? null : PartitionName.templatePrefix(tableName.schema(), tableName.name());
    }

    public List<List<String>> partitionedBy() {
        return Lists.map(partitionedByColumns, BoundCreateTable::toPartitionMapping);
    }

    public static List<String> toPartitionMapping(Symbol symbol) {
        String fqn = Symbols.pathFromSymbol(symbol).fqn();
        String typeMappingName = DataTypes.esMappingNameFrom(symbol.valueType().id());
        return List.of(fqn, typeMappingName);
    }

    public Map<String, String> getCheckConstraints() {
        Map<String, String> checksMapping = new LinkedHashMap<>();
        for (var entry: checks.entrySet()) {
            String constraintName = entry.getKey();
            AnalyzedCheck analyzedCheck = entry.getValue();
            checksMapping.put(constraintName, analyzedCheck.expression());
        }
        return checksMapping;
    }

    public IntArrayList primaryKeysIndices() {
        IntArrayList pkKeyIndices = new IntArrayList(primaryKeys.size());
        for (Reference pk : primaryKeys) {
            int idx = Reference.indexOf(columns.values(), pk.column());
            pkKeyIndices.add(idx);
        }
        return pkKeyIndices;
    }
}

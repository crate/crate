/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertFromValuesAnalyzedStatement extends AbstractInsertAnalyzedStatement {

    private final List<Symbol[]> onDuplicateKeyAssignments = new ArrayList<>();
    private final List<String[]> onDuplicateKeyAssignmentsColumns = new ArrayList<>();
    private final List<Object[]> sourceMaps = new ArrayList<>();
    private final List<Map<String, String>> partitionMaps = new ArrayList<>();

    private final List<String> ids = new ArrayList<>();
    private final List<String> routingValues = new ArrayList<>();
    private final List<Integer> bulkIndices = new ArrayList<>();

    private final int numBulkResponses;

    private int numAddedGeneratedColumns = 0;

    public InsertFromValuesAnalyzedStatement(DocTableInfo tableInfo, int numBulkResponses) {
        this.numBulkResponses = numBulkResponses;
        super.tableInfo(tableInfo);
        if (tableInfo.isPartitioned()) {
            for (Map<String, String> partitionMap : partitionMaps) {
                partitionMap = new HashMap<>(tableInfo.partitionedByColumns().size());
                for (Reference partInfo : tableInfo.partitionedByColumns()) {
                    // initialize with null values for missing partitioned columns
                    partitionMap.put(partInfo.column().name(), null);
                }
            }
        }
    }

    public List<Map<String, String>> partitionMaps() {
        return partitionMaps;
    }

    // create and add a new partition map
    public Map<String, String> newPartitionMap() {
        Map<String, String> map = new HashMap<>(tableInfo().partitionedByColumns().size());
        for (Reference partInfo : tableInfo().partitionedByColumns()) {
            // initialize with null values for missing partitioned columns
            map.put(partInfo.column().fqn(), null);
        }
        partitionMaps.add(map);
        return map;
    }

    @Nullable
    public Map<String, String> currentPartitionMap() {
        return partitionMaps.get(partitionMaps.size() - 1);
    }

    private List<String> partitionedByColumnNames() {
        assert tableInfo != null : "tableInfo must not be null";
        List<String> names = new ArrayList<>(tableInfo.partitionedByColumns().size());
        for (Reference info : tableInfo.partitionedByColumns()) {
            names.add(info.column().fqn());
        }
        return names;
    }

    public List<String> generatePartitions() {
        List<String> partitionValues = new ArrayList<>(partitionMaps.size());
        for (Map<String, String> map : partitionMaps) {
            List<BytesRef> values = new ArrayList<>(map.size());
            List<String> columnNames = partitionedByColumnNames();
            for (String columnName : columnNames) {
                values.add(BytesRefs.toBytesRef(map.get(columnName)));
            }
            PartitionName partitionName = new PartitionName(tableInfo().ident(), values);
            partitionValues.add(partitionName.asIndexName());
        }
        return partitionValues;
    }

    public List<Object[]> sourceMaps() {
        return sourceMaps;
    }

    protected void addIdAndRouting(String id, @Nullable String clusteredByValue) {
        ids.add(id);
        if (clusteredByValue == null) {
            routingValues.add(id);
        } else {
            routingValues.add(clusteredByValue);
        }
    }

    public void addOnDuplicateKeyAssignments(Symbol[] assignments) {
        assert assignments != null && assignments.length != 0 : "must have updateAssignments!";
        onDuplicateKeyAssignments.add(assignments);
    }

    public List<Symbol[]> onDuplicateKeyAssignments() {
        return onDuplicateKeyAssignments;
    }

    public void addOnDuplicateKeyAssignmentsColumns(String[] assignmentsColumns) {
        assert assignmentsColumns != null && assignmentsColumns.length != 0 : "must have updateAssignments columns!";
        onDuplicateKeyAssignmentsColumns.add(assignmentsColumns);
    }

    public List<String[]> onDuplicateKeyAssignmentsColumns() {
        return onDuplicateKeyAssignmentsColumns;
    }

    public List<String> ids() {
        return ids;
    }

    public List<String> routingValues() {
        return routingValues;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitInsertFromValuesStatement(this, context);
    }

    public int numBulkResponses() {
        return numBulkResponses;
    }

    public void addGeneratedColumn(Reference reference) {
        columns().add(reference);
        numAddedGeneratedColumns++;
    }

    public int numAddedGeneratedColumns() {
        return numAddedGeneratedColumns;
    }

    public List<Integer> bulkIndices() {
        return bulkIndices;
    }
}

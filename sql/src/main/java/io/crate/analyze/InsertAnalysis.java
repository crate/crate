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

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.IntSet;
import io.crate.PartitionName;
import io.crate.metadata.*;
import io.crate.planner.symbol.Reference;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertAnalysis extends AbstractDataAnalysis {

    private List<Reference> columns;
    private IntSet primaryKeyColumnIndices = new IntOpenHashSet(); // optional
    private IntSet partitionedByColumnsIndices = new IntOpenHashSet();
    private int routingColumnIndex = -1;

    private final List<Map<String, Object>> sourceMaps = new ArrayList<>();
    private List<Map<String, String>> partitionMaps = new ArrayList<>();

    public InsertAnalysis(ReferenceInfos referenceInfos,
                          Functions functions,
                          Object[] parameters,
                          ReferenceResolver referenceResolver) {
        super(referenceInfos, functions, parameters, referenceResolver);
    }

    @Override
    public void editableTable(TableIdent tableIdent) {
        super.editableTable(tableIdent);
        if (table().isPartitioned()) {
            for (Map<String, String> partitionMap : partitionMaps) {
                partitionMap = new HashMap<>(table().partitionedByColumns().size());
                for (ReferenceInfo partInfo : table().partitionedByColumns()) {
                    // initialize with null values for missing partitioned columns
                    partitionMap.put(partInfo.ident().columnIdent().name(), null);
                }
            }
        }
    }

    public List<Reference> columns() {
        return columns;
    }

    public IntSet partitionedByIndices() {
        return partitionedByColumnsIndices;
    }

    public void addPartitionedByIndex(int i) {
        this.partitionedByColumnsIndices.add(i);
    }

    public List<Map<String, String>> partitionMaps() {
        return partitionMaps;
    }

    // create and add a new partition map
    public Map<String, String> newPartitionMap() {
        Map<String, String> map = new HashMap<>(table().partitionedByColumns().size());
        for (ReferenceInfo partInfo : table().partitionedByColumns()) {
            // initialize with null values for missing partitioned columns
            map.put(partInfo.ident().columnIdent().fqn(), null);
        }
        partitionMaps.add(map);
        return map;
    }

    public List<String> partitions() {
        List<String> partitionValues = new ArrayList<>(partitionMaps.size());
        for (Map<String, String> map : partitionMaps) {
            List<String> values = new ArrayList<>(map.size());
            List<String> columnNames = partitionedByColumnNames();
            for (String columnName : columnNames) {
                values.add(map.get(columnName));
            }
            PartitionName partitionName = new PartitionName(
                table().ident().name(),
                values
            );
            partitionValues.add(partitionName.stringValue());
        }
        return partitionValues;
    }

    public void columns(List<Reference> columns) {
        this.columns = columns;
    }

    public IntSet primaryKeyColumnIndices() {
        return primaryKeyColumnIndices;
    }

    public void addPrimaryKeyColumnIdx(int primaryKeyColumnIdx) {
        this.primaryKeyColumnIndices.add(primaryKeyColumnIdx);
    }

    /**
     * TODO: use proper info from DocTableInfo when implemented
     * @return
     */
    private List<String> partitionedByColumnNames() {
        assert table != null;
        List<String> names = new ArrayList<>(table.partitionedByColumns().size());
        for (ReferenceInfo info : table.partitionedByColumns()) {
            names.add(info.ident().columnIdent().fqn());
        }
        return names;
    }

    public void routingColumnIndex(int routingColumnIndex) {
        this.routingColumnIndex = routingColumnIndex;
    }

    public int routingColumnIndex() {
        return routingColumnIndex;
    }

    public List<Map<String, Object>> sourceMaps() {
        return sourceMaps;
    }

    @Override
    public void addIdAndRouting(List<String> primaryKeyValues, String clusteredByValue) {
        addIdAndRouting(true, primaryKeyValues, clusteredByValue);
    }

    @Nullable
    @Override
    public ReferenceInfo getReferenceInfo(ReferenceIdent ident) {
        // fields of object arrays interpreted as they were created
        return referenceInfos.getReferenceInfo(ident);
    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitInsertAnalysis(this, context);
    }
}

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

import io.crate.PartitionName;
import io.crate.metadata.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertFromValuesAnalysis extends AbstractInsertAnalysis {

    private final List<BytesReference> sourceMaps = new ArrayList<>();
    private final List<Map<String, String>> partitionMaps = new ArrayList<>();

    private final AnalyzedTableElements analyzedTableElements = new AnalyzedTableElements();

    public InsertFromValuesAnalysis(ReferenceInfos referenceInfos,
                                    Functions functions,
                                    Analyzer.ParameterContext parameterContext,
                                    ReferenceResolver referenceResolver) {
        super(referenceInfos, functions, parameterContext, referenceResolver);
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

    public @Nullable Map<String, String> currentPartitionMap() {
        return partitionMaps.get(partitions().size()-1);
    }

    public List<String> partitions() {
        List<String> partitionValues = new ArrayList<>(partitionMaps.size());
        for (Map<String, String> map : partitionMaps) {
            List<BytesRef> values = new ArrayList<>(map.size());
            List<String> columnNames = partitionedByColumnNames();
            for (String columnName : columnNames) {
                values.add(BytesRefs.toBytesRef(map.get(columnName)));
            }
            PartitionName partitionName = new PartitionName(
                table().ident().name(),
                values
            );
            partitionValues.add(partitionName.stringValue());
        }
        return partitionValues;
    }

    public List<BytesReference> sourceMaps() {
        return sourceMaps;
    }

    public void addNewColumn(AnalyzedColumnDefinition column){
        analyzedTableElements.merge(column);
    }

    public Boolean mappingChanged() {
        return analyzedTableElements.columns().size() > 0;
    }

    public Map<String, Object> mapping() {
        return  analyzedTableElements.toMapping();
    }

    @Override
    public void addIdAndRouting(List<BytesRef> primaryKeyValues, String clusteredByValue) {
        addIdAndRouting(true, primaryKeyValues, clusteredByValue);
    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitInsertFromValuesAnalysis(this, context);
    }
}

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

package io.crate.metadata.doc;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.types.DataType;
import org.elasticsearch.common.collect.Tuple;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class PartitionedByMappingExtractor {

    private PartitionedByMappingExtractor() {
    }

    @SuppressWarnings("unchecked")
    public static Iterable<Tuple<ColumnIdent, DataType>> extractPartitionedByColumns(Map<String, Object> mapping) {
        Map<String, Object> metaMap = (Map<String, Object>) mapping.get("_meta");
        if (metaMap != null) {
            Object partitionedByColumnsMaybe = metaMap.get("partitioned_by");
            if (partitionedByColumnsMaybe instanceof List) {
                List<List<String>> partitionedByColumns = (List<List<String>>) partitionedByColumnsMaybe;
                return extractPartitionedByColumns(partitionedByColumns);
            }
        }
        return ImmutableList.of();
    }

    static Iterable<Tuple<ColumnIdent, DataType>> extractPartitionedByColumns(Collection<List<String>> partitionedByList) {
        return partitionedByList.stream()
            .map(PartitionedByMappingExtractor::toColumnIdentAndType)::iterator;
    }

    private static Tuple<ColumnIdent, DataType> toColumnIdentAndType(List<String> partitioned) {
        String dottedColumnName = partitioned.get(0);
        ColumnIdent column = ColumnIdent.fromPath(dottedColumnName);
        String typeDefinition = partitioned.get(1);
        DataType type = DocIndexMetaData.getColumnDataType(Collections.singletonMap("type", typeDefinition));
        return new Tuple<>(column, type);
    }
}

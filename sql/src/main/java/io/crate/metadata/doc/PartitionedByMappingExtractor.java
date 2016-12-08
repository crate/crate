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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.types.DataType;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PartitionedByMappingExtractor {

    private static final Function<List<String>, Tuple<ColumnIdent, DataType>> EXTRACTOR_FUNCTION = new Function<List<String>, Tuple<ColumnIdent, DataType>>() {
        @Nullable
        @Override
        public Tuple<ColumnIdent, DataType> apply(List<String> partitioned) {
            ColumnIdent ident = ColumnIdent.fromPath(partitioned.get(0));
            assert ident != null : "ident must not be null";
            DataType type = DocIndexMetaData.getColumnDataType(new MapBuilder<String, Object>().put("type", partitioned.get(1)).map());
            return new Tuple<>(ident, type);
        }
    };

    @SuppressWarnings("unchecked")
    public static Iterable<Tuple<ColumnIdent, DataType>> extractPartitionedByColumns(Map<String, Object> mapping) {
        Map<String, Object> metaMap = (Map<String, Object>) mapping.get("_meta");
        if (metaMap != null) {
            Object partitionedByColumnsMaybe = metaMap.get("partitioned_by");
            if (partitionedByColumnsMaybe != null && partitionedByColumnsMaybe instanceof List) {
                List<List<String>> partitionedByColumns = (List<List<String>>) partitionedByColumnsMaybe;
                return extractPartitionedByColumns(partitionedByColumns);
            }
        }
        return ImmutableList.of();
    }

    public static Iterable<Tuple<ColumnIdent, DataType>> extractPartitionedByColumns(Collection<List<String>> partitionedByList) {
        return FluentIterable.from(partitionedByList).transform(EXTRACTOR_FUNCTION);
    }
}

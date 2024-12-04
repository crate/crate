/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.reference.doc.lucene;

import java.util.List;
import java.util.Map;

import io.crate.common.collections.Maps;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;

public interface PartitionValueInjector {

    Map<String, Object> injectValues(Map<String, Object> input);

    static PartitionValueInjector create(String indexName, List<Reference> partitionColumns) {

        if (partitionColumns.isEmpty()) {
            return m -> m;
        }

        PartitionName partitionName = PartitionName.fromIndexOrTemplate(indexName);
        if (partitionName.values().size() != partitionColumns.size()) {
            throw new IllegalArgumentException("Partition values " + partitionName.values() + " from index " + indexName
                + " must match partition columns " + partitionColumns);
        }

        return input -> {
            for (int i = 0; i < partitionColumns.size(); i++) {
                ColumnIdent columnIdent = partitionColumns.get(i).column();
                Maps.mergeInto(input, columnIdent.name(), columnIdent.path(), partitionName.values().get(i));
            }
            return input;
        };
    }
}

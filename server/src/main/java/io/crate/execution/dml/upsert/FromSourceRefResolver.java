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

package io.crate.execution.dml.upsert;

import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.ValueExtractors;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.PartitionName;
import io.crate.metadata.SimpleReference;

import java.util.List;
import java.util.Map;

class FromSourceRefResolver implements ReferenceResolver<CollectExpression<Map<String, Object>, ?>> {

    static final FromSourceRefResolver WITHOUT_PARTITIONED_BY_REFS = new FromSourceRefResolver(List.of(), "");

    private final List<SimpleReference> partitionedBy;
    private final PartitionName partitionName;

    FromSourceRefResolver(List<SimpleReference> partitionedBy,
                          String indexName) {
        this.partitionedBy = partitionedBy;
        this.partitionName = partitionedBy.isEmpty()
            ? null
            : PartitionName.fromIndexOrTemplate(indexName);
    }

    @Override
    public CollectExpression<Map<String, Object>, Object> getImplementation(SimpleReference ref) {
        int partitionPos = partitionedBy.indexOf(ref);
        if (partitionPos >= 0 && !(ref instanceof GeneratedReference)) {
            return NestableCollectExpression
                .constant(partitionName.values().get(partitionPos));
        } else {
            return NestableCollectExpression
                .forFunction(ValueExtractors.fromMap(ref.column(), ref.valueType()));
        }
    }
}

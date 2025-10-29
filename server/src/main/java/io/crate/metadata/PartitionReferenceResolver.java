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

package io.crate.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.reference.partitioned.PartitionExpression;

public class PartitionReferenceResolver implements ReferenceResolver<PartitionExpression> {

    private final Map<ColumnIdent, PartitionExpression> expressionMap;
    private final List<PartitionExpression> partitionExpressions;

    public PartitionReferenceResolver(List<PartitionExpression> partitionExpressions) {
        this.partitionExpressions = partitionExpressions;
        this.expressionMap = new HashMap<>(partitionExpressions.size(), 1.0f);
        for (PartitionExpression partitionExpression : partitionExpressions) {
            expressionMap.put(partitionExpression.reference().column(), partitionExpression);
        }
    }

    @Override
    public PartitionExpression getImplementation(Reference ref) {
        PartitionExpression expression = expressionMap.get(ref.column());
        assert expression != null : "granularity < PARTITION should have been resolved already";
        return expression;
    }

    public List<PartitionExpression> expressions() {
        return partitionExpressions;
    }
}

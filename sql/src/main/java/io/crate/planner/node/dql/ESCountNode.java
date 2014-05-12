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

package io.crate.planner.node.dql;

import io.crate.analyze.WhereClause;
import io.crate.planner.node.PlanVisitor;
import io.crate.types.DataType;
import io.crate.types.LongType;

import java.util.Arrays;
import java.util.List;

public class ESCountNode extends ESDQLPlanNode {

    private final List<DataType> outputTypes = Arrays.<DataType>asList(LongType.INSTANCE);
    private final String indexName;
    private final WhereClause whereClause;

    public ESCountNode(String indexName, WhereClause whereClause) {
        this.indexName = indexName;
        this.whereClause = whereClause;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitESCountNode(this, context);
    }

    public String indexName() {
        return indexName;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    @Override
    public List<DataType> outputTypes() {
        return outputTypes;
    }
}

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

package io.crate.planner.node.dml;

import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.bytes.BytesReference;

import javax.annotation.Nullable;
import java.util.List;

/**
 * plan node for 1 or more documents to index via ESIndexTask
 * for a single index
 */
public class ESIndexNode extends DMLPlanNode {

    private final boolean partitionedTable;
    private final boolean isBulkRequest;
    private final String[] indices;

    private final List<BytesReference> sources;
    private final List<String> ids;
    private final List<String> routingValues;

    public ESIndexNode(String[] indices,
                       List<BytesReference> sources,
                       List<String> ids,
                       @Nullable List<String> routingValues,
                       boolean partitionedTable,
                       boolean isBulkRequest) {
        this.partitionedTable = partitionedTable;
        this.isBulkRequest = isBulkRequest;
        assert indices != null : "no indices";
        assert indices.length == 1 || indices.length == sources.size() : "unsupported number of indices";
        this.indices = indices;
        this.sources = sources;
        this.ids = ids;
        this.routingValues = routingValues;
    }

    public String[] indices() {
        return indices;
    }

    public List<BytesReference> sourceMaps() {
        return sources;
    }

    public List<String> ids() {
        return ids;
    }

    @Nullable
    public List<String> routingValues() {
        return routingValues;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitESIndexNode(this, context);
    }

    @Override
    public void addProjection(Projection projection) {
        throw new UnsupportedOperationException("addProjection not supported");
    }

    /**
     * @return true if the table is partitioned, otherwise false
     */
    public boolean partitionedTable() {
        return partitionedTable;
    }

    public boolean isBulkRequest() {
        return isBulkRequest;
    }
}

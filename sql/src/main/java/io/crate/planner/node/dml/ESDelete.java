/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.analyze.where.DocKeys;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.PlanVisitor;
import io.crate.planner.UnnestablePlan;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ESDelete extends UnnestablePlan {

    private final UUID jobId;
    private final DocTableInfo tableInfo;
    private final List<DocKeys.DocKey> docKeys;
    private final Map<Integer, Integer> itemToBulkIdx;
    private final int bulkSize;

    public ESDelete(UUID jobId,
                    DocTableInfo tableInfo,
                    List<DocKeys.DocKey> docKeys,
                    Map<Integer, Integer> itemToBulkIdx,
                    int bulkSize) {
        this.jobId = jobId;
        this.tableInfo = tableInfo;
        this.docKeys = docKeys;
        this.itemToBulkIdx = itemToBulkIdx;
        this.bulkSize = bulkSize;
    }

    public DocTableInfo tableInfo() {
        return tableInfo;
    }

    public List<DocKeys.DocKey> docKeys() {
        return docKeys;
    }

    public Map<Integer, Integer> getItemToBulkIdx() {
        return itemToBulkIdx;
    }

    public int getBulkSize() {
        return bulkSize;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitESDelete(this, context);
    }

    @Override
    public UUID jobId() {
        return jobId;
    }
}

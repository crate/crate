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

package org.cratedb.action.groupby;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.cratedb.action.sql.OrderByColumnIdx;

import java.util.Comparator;

public class GroupByRowComparator implements Comparator<GroupByRow> {

    private final OrderByColumnIdx[] orderByIndices;
    private final GroupByFieldExtractor[] extractors;

    public GroupByRowComparator(GroupByFieldExtractor[] groupByFieldExtractors,
                                OrderByColumnIdx[] orderByIndices) {
        this.extractors = groupByFieldExtractors;
        this.orderByIndices = orderByIndices;
    }

    @Override
    public int compare(GroupByRow o1, GroupByRow o2) {
        ComparisonChain chain = ComparisonChain.start();
        for (OrderByColumnIdx orderByIndex : orderByIndices) {
            Object left = extractors[orderByIndex.index].getValue(o1);
            Object right = extractors[orderByIndex.index].getValue(o2);

            if (left != null && right != null) {
                chain = chain.compare((Comparable)left, (Comparable)right, orderByIndex.ordering);
            } else if (right != null) {
                chain = chain.compare(0, 1);
            } else if (left != null) {
                chain = chain.compare(1, 0);
            } else {
                chain = chain.compare(0, 0);
            }
        }

        return chain.result();
    }
}

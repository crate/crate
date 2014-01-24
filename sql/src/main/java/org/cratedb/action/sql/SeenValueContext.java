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

package org.cratedb.action.sql;

import org.cratedb.DataType;
import org.cratedb.action.collect.Expression;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SeenValueContext {

    private List<Integer> idxMap = new ArrayList<>();
    private List<Expression> distinctExpressions = new ArrayList<>();
    private DataType.Streamer[] streamers;

    public SeenValueContext(List<AggExpr> aggExprs) {

        for (AggExpr aggExpr : aggExprs) {
            if (aggExpr.isDistinct) {
                if (!distinctExpressions.contains(aggExpr.expression)) {
                    distinctExpressions.add(aggExpr.expression);
                }
                idxMap.add(distinctExpressions.indexOf(aggExpr.expression));
            }
        }

        streamers = new DataType.Streamer[distinctExpressions.size()];
        int idx = 0;
        for (Expression distinctExpression : distinctExpressions) {
            streamers[idx] = distinctExpression.returnType().streamer();
            idx++;
        }
    }

    public List<Set<Object>> createList() {
        List<Set<Object>> seenValuesList = new ArrayList<>();
        for (Expression distinctExpression : distinctExpressions) {
            seenValuesList.add(new HashSet<>());
        }
        return seenValuesList;
    }

    public int mappedIdx(int idx) {
        return idxMap.get(idx);
    }

    public void writeTo(StreamOutput out, List<Set<Object>> seenValuesList) throws IOException {
        for (int i = 0; i < streamers.length; i++) {
            Set seenValues = seenValuesList.get(i);
            out.writeVInt(seenValues.size());
            for (Object seenValue : seenValues) {
                streamers[i].writeTo(out, seenValue);
            }
        }
    }

    public List<Set<Object>> readFrom(StreamInput in) throws IOException {
        List<Set<Object>> list = createList();
        for (int i = 0; i < list.size(); i++) {
            int length = in.readVInt();
            for (int j = 0; j < length; j++) {
                list.get(i).add(streamers[i].readFrom(in));
            }
        }

        return list;
    }


    public boolean queryContainsDistinct() {
        return idxMap.size() > 0;
    }
}

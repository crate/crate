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

import com.google.common.base.Joiner;
import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggState;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Represents the resulting row of a "select with group by" statement.
 *
 * Assuming a query as follows:
 *
 *      select count(*), x, avg(*), y from... group by x, y
 *
 * Then the data is structured in the following form:
 *
 *  AggegrateStates: [ CountAggState, AvgAggState ]
 *  GroupByKey: (Object[]) { "ValueX", "ValueY" }
 */
public class GroupByRow {

    public List<Set<Object>> seenValuesList;
    public GroupByKey key;

    public List<AggState> aggStates;
    public boolean[] continueCollectingFlags;  // not serialized, only for collecting

    public GroupByRow() {
    }

    public GroupByRow(GroupByKey key, List<AggState> aggStates, List<Set<Object>> seenValuesList)
    {
        this.key = key;
        this.aggStates = aggStates;
        assert seenValuesList != null;
        this.seenValuesList = seenValuesList;
        this.continueCollectingFlags = new boolean[aggStates != null ? aggStates.size() : 0];
        Arrays.fill(this.continueCollectingFlags, true);
    }

    /**
     * use this ctor only for testing as serialization won't work because the aggExpr and seenValues are missing!
     */
    public GroupByRow(GroupByKey key, List<AggState> aggStates) {
        this(key, aggStates, new ArrayList<Set<Object>>(0));
    }

    public static GroupByRow createEmptyRow(GroupByKey key, ParsedStatement stmt) {
        List<Set<Object>> seenValuesList = stmt.seenValueContext().createList();
        List<AggState> aggStates = new ArrayList<>(stmt.aggregateExpressions().size());
        AggState aggState;

        if (stmt.seenValueContext().queryContainsDistinct()) {
            int idx = 0;
            for (AggExpr aggExpr : stmt.aggregateExpressions()) {
                aggState = aggExpr.createAggState();
                if (aggExpr.isDistinct) {
                    aggState.setSeenValuesRef(seenValuesList.get(stmt.seenValueContext().mappedIdx(idx)));
                    idx++;
                }
                aggStates.add(aggState);
            }
        } else {
            for (AggExpr aggExpr : stmt.aggregateExpressions()) {
                aggStates.add(aggExpr.createAggState());
            }
        }

        return new GroupByRow(key, aggStates, seenValuesList);
    }

    @Override
    public String toString() {
        return "GroupByRow{" +
            "aggregateStates=" + Joiner.on(", ").join(aggStates) +
            ", key=" + key +
            '}';
    }

    public int size() {
        return key.size() + aggStates.size();
    }

    @SuppressWarnings("unchecked")
    public synchronized void merge(GroupByRow otherRow) {

        // merge of he seenValues is done before-hand so that the aggStates don't have to in their
        // reduce method.
        // this avoids doing the merge twice if aggStates share the seenValues.
        int idx = 0;
        assert seenValuesList.size() == otherRow.seenValuesList.size();
        for (Set<Object> seenValues : seenValuesList) {
            seenValues.addAll(otherRow.seenValuesList.get(idx));
            idx++;
        }

        for (int i = 0; i < aggStates.size(); i++) {
            aggStates.get(i).reduce(otherRow.aggStates.get(i));
        }
    }


    public static GroupByRow readGroupByRow(ParsedStatement stmt, DataType.Streamer[] keyStreamers,
                                            StreamInput in) throws IOException {
        GroupByRow row = new GroupByRow();
        Object[] keyValue = new Object[keyStreamers.length];
        for (int i = 0; i < keyValue.length; i++) {
            keyValue[i] = keyStreamers[i].readFrom(in);
        }
        row.readFrom(in, new GroupByKey(keyValue), stmt);
        return row;
    }

    public void readFrom(StreamInput in, GroupByKey key, ParsedStatement stmt) throws
            IOException {
        this.key = key;
        readStates(in, stmt);
    }

    public void readStates(StreamInput in, ParsedStatement stmt) throws IOException {
        seenValuesList = stmt.seenValueContext().readFrom(in);

        aggStates = new ArrayList<>(stmt.aggregateExpressions().size());
        AggExpr aggExpr;
        int seenIdxIndex = 0;
        for (int i = 0; i < stmt.aggregateExpressions().size(); i++) {
            aggExpr = stmt.aggregateExpressions().get(i);
            aggStates.add(i, aggExpr.createAggState());
            aggStates.get(i).readFrom(in);
            if (aggExpr.isDistinct) {
                aggStates.get(i).setSeenValuesRef(seenValuesList.get(stmt.seenValueContext().mappedIdx(seenIdxIndex)));
                seenIdxIndex++;
            }
        }
    }

    public void writeTo(DataType.Streamer[] keyStreamers, ParsedStatement stmt, StreamOutput out)
            throws IOException {
        for (int i = 0; i < keyStreamers.length; i++) {
            keyStreamers[i].writeTo(out, key.get(i));
        }
        writeStates(out, stmt);
    }

    public void writeStates(StreamOutput out, ParsedStatement stmt) throws IOException {
        stmt.seenValueContext().writeTo(out, seenValuesList);

        for (AggState aggState : aggStates) {
            aggState.writeTo(out);
        }
    }


    public void terminatePartial() {
        for (AggState aggState : aggStates) {
            aggState.terminatePartial();
        }

        for (Set<Object> seenValues : seenValuesList) {
            seenValues.clear();
        }
    }
}

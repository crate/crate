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

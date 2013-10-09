package org.cratedb.action;

import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Result of a group by operation.
 * Each key represents a row for the SQLResponse.
 *
 * See {@link TransportDistributedSQLAction} for an overview of the workflow how the SQLGroupByResult is used.
 */
public class SQLGroupByResult implements Streamable {

    public Map<Object, GroupByRow> result = newHashMap();

    public SQLGroupByResult() {
        // empty ctor - serialization
    }

    public SQLGroupByResult(Map<Object, GroupByRow> result) {
        this.result = result;
    }

    public void merge(SQLGroupByResult otherResult) {
        merge(otherResult.result);
    }

    protected void merge(Map<Object, GroupByRow> mapperResult) {
        for (Map.Entry<Object, GroupByRow> entry : mapperResult.entrySet()) {
            GroupByRow currentRow = result.get(entry.getKey());
            if (currentRow == null) {
                result.put(entry.getKey(), entry.getValue());
            } else {
                GroupByRow otherRow = entry.getValue();
                assert currentRow.size() == otherRow.size();

                for (Map.Entry<Integer, AggState> aggEntry : otherRow.aggregateStates.entrySet()) {
                    AggState currentState = currentRow.aggregateStates.get(aggEntry.getKey());
                    currentState.merge(aggEntry.getValue());
                }
            }
        }
    }

    public int size() {
        return result.size();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int mapSize = in.readVInt();
        result = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            Object key = in.readGenericValue();
            result.put(key, GroupByRow.readGroupByRow(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(result.size());
        for (Map.Entry<Object, GroupByRow> entry : result.entrySet()) {
            out.writeGenericValue(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    public static SQLGroupByResult readSQLGroupByResult(StreamInput in) throws IOException {
        SQLGroupByResult result = new SQLGroupByResult();
        result.readFrom(in);
        return result;
    }
}

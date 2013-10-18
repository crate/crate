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

    public Map<Integer, GroupByRow> result = newHashMap();

    public SQLGroupByResult() {
        // empty ctor - serialization
    }

    public SQLGroupByResult(Map<Integer, GroupByRow> result) {
        this.result = result;
    }

    public void merge(SQLGroupByResult otherResult) {
        merge(otherResult.result);
    }

    /**
     * merge the content of "mapperResult" into "result"
     *
     * a an entry (identified by key) in mapperResult that is missing in result is added to result
     * if the entry is in result the values are merged.
     * @param mapperResult
     */
    protected void merge(Map<Integer, GroupByRow> mapperResult) {
        for (Map.Entry<Integer, GroupByRow> entry : mapperResult.entrySet()) {
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
            int key = in.readInt();
            result.put(key, GroupByRow.readGroupByRow(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(result.size());
        for (Map.Entry<Integer, GroupByRow> entry : result.entrySet()) {
            out.writeInt(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    public static SQLGroupByResult readSQLGroupByResult(StreamInput in) throws IOException {
        SQLGroupByResult result = new SQLGroupByResult();
        result.readFrom(in);
        return result;
    }
}

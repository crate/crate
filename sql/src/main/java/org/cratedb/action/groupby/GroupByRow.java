package org.cratedb.action.groupby;

import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.AggState;
import org.cratedb.action.groupby.aggregate.AggStateReader;
import org.cratedb.action.parser.ColumnDescription;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Represents the resulting row of a "select with group by" statement.
 *
 * Assuming a query as follows:
 *
 *      select count(*), x, avg(*), y from... group by x, y
 *
 * Then the data is structured in the following form:
 *
 *  AggegrateStates: {
 *      "0": CountAggState,
 *      "2": AvgAggState
 *  }
 *
 *  RegularColumns: {
 *      "1": "ValueX",
 *      "3": "ValueY"
 *  }
 *
 *  AggregateStates and RegularColumn states are split because only the States have to be merged
 *  when reducing {@link org.cratedb.action.SQLGroupByResult}
 *
 *  The key contains the column index number so that the SQLResponse's row can be built in the correct order.
 */
public class GroupByRow implements Streamable {

    public Map<Integer, AggState> aggregateStates = newHashMap();
    public Map<Integer, Object> regularColumns = newHashMap();

    public static GroupByRow createEmptyRow(List<ColumnDescription> columns,
                                            Map<String, AggFunction> aggregateFunctions) {
        GroupByRow row = new GroupByRow();
        int columnIdx = -1;
        for (ColumnDescription column : columns) {
            columnIdx++;
            switch (column.type) {
                case ColumnDescription.Types.AGGREGATE_COLUMN:
                    row.aggregateStates.put(
                        columnIdx,
                        aggregateFunctions.get(((AggExpr) column).functionName).createAggState()
                    );
                    break;
                case ColumnDescription.Types.CONSTANT_COLUMN:
                    row.regularColumns.put(columnIdx, null);
                    break;
            }
        }

        return row;
    }

    @Override
    public String toString() {
        return "GroupByRow{" +
            "aggregateStates=" + aggregateStates +
            ", regularColumns=" + regularColumns +
            '}';
    }

    public Object get(int columnIndex) {
        AggState aggState = aggregateStates.get(columnIndex);
        if (aggState == null) {
            return regularColumns.get(columnIndex);
        }

        return aggState;
    }

    public int size() {
        return aggregateStates.size() + regularColumns.size();
    }

    public static GroupByRow readGroupByRow(StreamInput in) throws IOException {
        GroupByRow row = new GroupByRow();
        row.readFrom(in);
        return row;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int numAggregateStates = in.readVInt();
        aggregateStates = new HashMap<>(numAggregateStates);
        for (int i = 0; i < numAggregateStates; i++) {
            int key = in.readVInt();
            AggState state = AggStateReader.readFrom(in);
            aggregateStates.put(key, state);
        }

        int numRegularColumns = in.readVInt();
        regularColumns = new HashMap<>(numRegularColumns);
        for (int i = 0; i < numRegularColumns; i++) {
            int key = in.readVInt();
            Object value = in.readGenericValue();
            regularColumns.put(key, value);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(aggregateStates.size());
        for (Map.Entry<Integer, AggState> entry : aggregateStates.entrySet()) {
            out.writeVInt(entry.getKey());
            entry.getValue().writeTo(out);
        }

        out.writeVInt(regularColumns.size());
        for (Map.Entry<Integer, Object> entry : regularColumns.entrySet()) {
            out.writeVInt(entry.getKey());
            out.writeGenericValue(entry.getValue());
        }
    }

    public void merge(GroupByRow otherRow) {
        for (Map.Entry<Integer, AggState> thisEntry : aggregateStates.entrySet()) {
            AggState currentValue = thisEntry.getValue();
            AggState otherValue = otherRow.aggregateStates.get(thisEntry.getKey());
            currentValue.merge(otherValue);
            thisEntry.setValue(currentValue);
        }
    }
}

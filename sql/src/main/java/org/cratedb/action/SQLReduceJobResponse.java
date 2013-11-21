package org.cratedb.action;

import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class SQLReduceJobResponse extends ActionResponse {

    private ParsedStatement parsedStatement;
    private Map<String,AggFunction> aggFunctionMap;
    public GroupByRow[] result;


    public SQLReduceJobResponse(Map<String, AggFunction> aggFunctionMap,
                                ParsedStatement parsedStatement) {
        this.aggFunctionMap = aggFunctionMap;
        this.parsedStatement = parsedStatement;
    }

    public SQLReduceJobResponse(SQLReduceJobStatus jobStatus) {
        this.result = jobStatus.toSortedArray(jobStatus.groupByResult);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        result = new GroupByRow[in.readVInt()];
        for (int i = 0; i < result.length; i++) {
            result[i] = GroupByRow.readGroupByRow(
                aggFunctionMap, parsedStatement.aggregateExpressions, in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(result.length);
        for (GroupByRow row : result) {
            row.writeTo(out);
        }
    }
}

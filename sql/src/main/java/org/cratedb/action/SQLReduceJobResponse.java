package org.cratedb.action;

import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.*;

public class SQLReduceJobResponse extends ActionResponse {

    private ParsedStatement parsedStatement;
    public Collection<GroupByRow> result;


    public SQLReduceJobResponse(ParsedStatement parsedStatement) {
        this.parsedStatement = parsedStatement;
    }

    public SQLReduceJobResponse(SQLReduceJobStatus jobStatus) {
        this.result = jobStatus.trimRows(jobStatus.reducedResult.values());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int resultLength = in.readVInt();
        result = new ArrayList<>(resultLength);
        for (int i = 0; i < resultLength; i++) {
            result.add(GroupByRow.readGroupByRow(parsedStatement.aggregateExpressions, in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(result.size());
        for (GroupByRow row : result) {
            row.writeTo(out);
        }
    }
}

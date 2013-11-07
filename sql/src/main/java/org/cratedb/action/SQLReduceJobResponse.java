package org.cratedb.action;

import org.cratedb.action.groupby.GroupByRow;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.PriorityQueue;

public class SQLReduceJobResponse extends ActionResponse {

    public GroupByRow[] result;

    public SQLReduceJobResponse() {
    }

    public SQLReduceJobResponse(SQLReduceJobStatus jobStatus) {
        this.result = jobStatus.toSortedArray(jobStatus.groupByResult);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        result = new GroupByRow[in.readVInt()];
        for (int i = 0; i < result.length; i++) {
            result[i] = GroupByRow.readGroupByRow(in);
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

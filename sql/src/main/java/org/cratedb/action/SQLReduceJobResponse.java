package org.cratedb.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class SQLReduceJobResponse extends ActionResponse {

    public SQLGroupByResult result;

    public SQLReduceJobResponse() {
    }

    public SQLReduceJobResponse(SQLGroupByResult result) {
        this.result = result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        result.writeTo(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        result = SQLGroupByResult.readSQLGroupByResult(in);
    }
}

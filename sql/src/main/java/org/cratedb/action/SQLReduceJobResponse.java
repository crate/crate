package org.cratedb.action;

import org.cratedb.DataType;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class SQLReduceJobResponse extends ActionResponse {

    private final ParsedStatement parsedStatement;
    private Collection<GroupByRow> rows;

    public SQLReduceJobResponse(ParsedStatement parsedStatement) {
        this.parsedStatement = parsedStatement;
    }

    public SQLReduceJobResponse(Collection<GroupByRow> rows, ParsedStatement stmt) {
        this(stmt);
        this.rows = rows;
    }

    private DataType.Streamer[] getKeyStreamers() {
        DataType.Streamer[] keyStreamers = new DataType.Streamer[
                parsedStatement.groupByExpressions().size()];
        for (int i = 0; i < keyStreamers.length; i++) {
            keyStreamers[i] = parsedStatement.groupByExpressions().get(i).returnType().streamer();
        }
        return keyStreamers;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int resultLength = in.readVInt();
        if (resultLength == 0) {
            return;
        }
        DataType.Streamer[] streamers = getKeyStreamers();
        rows = new ArrayList<>(resultLength);
        for (int i = 0; i < resultLength; i++) {
            rows.add(GroupByRow.readGroupByRow(parsedStatement, streamers, in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (rows == null || rows.size() == 0) {
            out.writeVInt(0);
            return;
        }
        DataType.Streamer[] streamers = getKeyStreamers();
        out.writeVInt(rows.size());
        for (GroupByRow row : rows) {
            row.writeTo(streamers, out);
        }
    }

    public Collection<GroupByRow> rows() {
        return rows;
    }
}

package org.cratedb.action;

import org.cratedb.action.sql.SQLRequest;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.UUID;


/**
 * Sent from a Handler to a Mapper Node to query a shard and then send the results to Reducer Nodes
 *
 * See {@link TransportDistributedSQLAction} for a full overview of the workflow.
 */
public class SQLShardRequest extends TransportRequest implements Streamable {

    public String[] reducers;
    public SQLRequest sqlRequest;
    public UUID contextId;
    public int shardId;


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = in.readVInt();
        contextId = new UUID(in.readLong(), in.readLong());
        reducers = in.readStringArray();
        sqlRequest = new SQLRequest();
        sqlRequest.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shardId);
        out.writeLong(contextId.getMostSignificantBits());
        out.writeLong(contextId.getLeastSignificantBits());
        out.writeStringArray(reducers);
        sqlRequest.writeTo(out);
    }
}

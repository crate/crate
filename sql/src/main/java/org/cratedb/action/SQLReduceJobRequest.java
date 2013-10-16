package org.cratedb.action;

import org.cratedb.action.sql.OrderByColumnIdx;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.UUID;

/**
 * sent from a Handler to a Reducer Node to create a reduce-job-context
 * and to inform the reducer that it will receive {@link #expectedShardResults}
 * number of {@link SQLMapperResultRequest} from mapper nodes
 *
 * See {@link TransportDistributedSQLAction} for a full overview of the workflow.
 */
public class SQLReduceJobRequest extends TransportRequest {

    public UUID contextId;
    public int expectedShardResults;
    public Integer limit;
    public OrderByColumnIdx[] orderByIndices;

    public SQLReduceJobRequest() {

    }

    public SQLReduceJobRequest(UUID contextId, int expectedShardResults,
                               Integer limit, OrderByColumnIdx[] orderByIndices) {
        this.contextId = contextId;
        this.expectedShardResults = expectedShardResults;
        this.limit = limit;
        this.orderByIndices = orderByIndices;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        contextId = new UUID(in.readLong(), in.readLong());
        expectedShardResults = in.readVInt();
        orderByIndices = new OrderByColumnIdx[in.readVInt()];
        for (int i = 0; i < orderByIndices.length; i++) {
            orderByIndices[i] = OrderByColumnIdx.readFromStream(in);
        }

        if (in.readBoolean()) {
            limit = in.readVInt();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(contextId.getMostSignificantBits());
        out.writeLong(contextId.getLeastSignificantBits());
        out.writeVInt(expectedShardResults);
        out.writeVInt(orderByIndices.length);
        for (OrderByColumnIdx index : orderByIndices) {
            index.writeTo(out);
        }

        if (limit != null) {
            out.writeBoolean(true);
            out.writeVInt(limit);
        } else {
            out.writeBoolean(false);
        }
    }
}

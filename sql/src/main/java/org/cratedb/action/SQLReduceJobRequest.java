package org.cratedb.action;

import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.sql.OrderByColumnIdx;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
    public Integer[] idxMap;
    public List<AggExpr> aggregateExpressions;


    public SQLReduceJobRequest() {

    }

    public SQLReduceJobRequest(UUID contextId, int expectedShardResults,
                               Integer limit, Integer[] idxMap, OrderByColumnIdx[] orderByIndices,
                               List<AggExpr> aggExprs) {
        this.contextId = contextId;
        this.expectedShardResults = expectedShardResults;
        this.limit = limit;
        this.orderByIndices = orderByIndices;
        this.idxMap = idxMap;
        this.aggregateExpressions = aggExprs;
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

        int aggExprSize = in.readVInt();
        aggregateExpressions = new ArrayList<>(aggExprSize);
        for (int i = 0; i < aggExprSize; i++) {
            aggregateExpressions.add(AggExpr.readFromStream(in));
        }

        idxMap = new Integer[in.readVInt()];
        for (int i = 0; i < idxMap.length; i++) {
            idxMap[i] = in.readVInt();
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

        out.writeVInt(aggregateExpressions.size());
        for (AggExpr aggExpr : aggregateExpressions) {
            aggExpr.writeTo(out);
        }

        out.writeVInt(idxMap.length);
        for (Integer idx : idxMap) {
            out.writeVInt(idx);
        }

        if (limit != null) {
            out.writeBoolean(true);
            out.writeVInt(limit);
        } else {
            out.writeBoolean(false);
        }
    }
}

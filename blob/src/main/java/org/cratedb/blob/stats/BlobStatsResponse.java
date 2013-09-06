package org.cratedb.blob.stats;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.*;

public class BlobStatsResponse extends BroadcastOperationResponse implements ToXContent {

    private BlobStatsShardResponse[] blobShardStats;

    public BlobStatsShardResponse[] blobShardStats() {
        return blobShardStats;
    }

    public BlobStatsResponse(BlobStatsShardResponse[] responses, int totalShards, int successfulShards,
                             int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.blobShardStats = responses;
    }

    public BlobStatsResponse() {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(blobShardStats.length);
        for (BlobStatsShardResponse shardStat : blobShardStats) {
            shardStat.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        blobShardStats = new BlobStatsShardResponse[in.readVInt()];
        for (int i = 0; i < blobShardStats.length; i++) {
            blobShardStats[i].readFrom(in);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        Map<String, Map<Integer, List<BlobStatsShardResponse>>> groupedShards =
            new HashMap<String, Map<Integer, List<BlobStatsShardResponse>>>();

        // group by index name and then by shard id
        for (BlobStatsShardResponse shardResponse : blobShardStats) {
            Map<Integer, List<BlobStatsShardResponse>> shardMapReference = groupedShards.get(shardResponse.getIndex());
            if (shardMapReference == null) {
                shardMapReference = new HashMap<Integer, List<BlobStatsShardResponse>>();
                groupedShards.put(shardResponse.getIndex(), shardMapReference);
            }

            List<BlobStatsShardResponse> shards = shardMapReference.get(shardResponse.getShardId());
            if (shards == null) {
                shards = new ArrayList<BlobStatsShardResponse>();
                shardMapReference.put(shardResponse.getShardId(), shards);
            }
            shards.add(shardResponse);
        }

        builder.startObject(Fields.INDICES);
        for (String index : groupedShards.keySet()) {
            long sum_size = 0;
            long sum_num_blobs = 0;
            long sum_primary_size = 0;
            builder.startObject(index);

            builder.startObject(Fields.SHARDS);
            for (Integer shardId : groupedShards.get(index).keySet()) {
                builder.startArray(shardId.toString());

                for (BlobStatsShardResponse shard : groupedShards.get(index).get(shardId)) {
                    builder.startObject();

                    builder.field(Fields.BLOBS);
                    builder = shard.blobStats().toXContent(builder, params);
                    builder.field(Fields.ROUTING);
                    builder = shard.shardRouting().toXContent(builder, params);

                    if (shard.shardRouting().primary()) {
                        sum_primary_size += shard.blobStats().totalUsage();
                        sum_num_blobs += shard.blobStats().count();
                    }
                    sum_size += shard.blobStats().totalUsage();

                    builder.endObject();
                }

                builder.endArray();
            }
            builder.endObject();
            builder.startObject(Fields.BLOBS)
                .field(Fields.PRIMARY_SIZE, sum_primary_size)
                .field(Fields.SIZE, sum_size)
                .field(Fields.NUM_BLOBS, sum_num_blobs)
                .endObject();
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }


    static final class Fields {
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
        static final XContentBuilderString BLOBS = new XContentBuilderString("blobs");
        static final XContentBuilderString ROUTING = new XContentBuilderString("routing");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
        static final XContentBuilderString PRIMARY_SIZE = new XContentBuilderString("primary_size");
        static final XContentBuilderString SIZE = new XContentBuilderString("size");
        static final XContentBuilderString NUM_BLOBS = new XContentBuilderString("count");
    }
}

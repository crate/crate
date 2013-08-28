package crate.elasticsearch.action.export;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;


/**
 * Internal export request executed directly against a specific index shard.
 */
class ShardExportRequest extends BroadcastShardOperationRequest {

    private BytesReference source;

    private String[] types = Strings.EMPTY_ARRAY;

    @Nullable
    private String[] filteringAliases;

    ShardExportRequest() {

    }

    public ShardExportRequest(String index, int shardId, @Nullable String[] filteringAliases, ExportRequest request) {
        super(index, shardId, request);
        this.source = request.source();
        this.types = request.types();
        this.filteringAliases = filteringAliases;
    }

    public BytesReference source() {
        return source;
    }

    public String[] types() {
        return this.types;
    }

    public String[] filteringAliases() {
        return filteringAliases;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        source = in.readBytesReference();
        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readString();
            }
        }
        int aliasesSize = in.readVInt();
        if (aliasesSize > 0) {
            filteringAliases = new String[aliasesSize];
            for (int i = 0; i < aliasesSize; i++) {
                filteringAliases[i] = in.readString();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(source);

        out.writeVInt(types.length);
        for (String type : types) {
            out.writeString(type);
        }
        if (filteringAliases != null) {
            out.writeVInt(filteringAliases.length);
            for (String alias : filteringAliases) {
                out.writeString(alias);
            }
        } else {
            out.writeVInt(0);
        }
    }
}

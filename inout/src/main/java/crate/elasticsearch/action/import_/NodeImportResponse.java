package crate.elasticsearch.action.import_;

import java.io.IOException;

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import crate.elasticsearch.import_.Importer;

public class NodeImportResponse extends NodeOperationResponse implements ToXContent {

    private Importer.Result result;

    NodeImportResponse() {
    }

    public NodeImportResponse(DiscoveryNode discoveryNode, Importer.Result result) {
        super(discoveryNode);
        this.result = result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params)
            throws IOException {
        builder.startObject();
        builder.field(Fields.NODE_ID, this.getNode().id());
        builder.field(Fields.TOOK, result.took);
        builder.startArray(Fields.IMPORTED_FILES);
        for (Importer.ImportCounts counts : result.importCounts) {
            builder.startObject();
            builder.field(Fields.FILE_NAME, counts.fileName);
            builder.field(Fields.SUCCESSES, counts.successes);
            builder.field(Fields.FAILURES, counts.failures);
            if (counts.invalid > 0) {
                builder.field(Fields.INVALIDATED, counts.invalid);
            }
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        result = new Importer.Result();
        result.took = in.readLong();
        int fileCount = in.readInt();
        for (int i = 0; i < fileCount; i++) {
            Importer.ImportCounts counts = new Importer.ImportCounts();
            counts.fileName = in.readString();
            counts.successes = in.readInt();
            counts.failures = in.readInt();
            counts.invalid = in.readInt();
            result.importCounts.add(counts);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(result.took);
        out.writeInt(result.importCounts.size());
        for (Importer.ImportCounts counts : result.importCounts) {
            out.writeString(counts.fileName);
            out.writeInt(counts.successes);
            out.writeInt(counts.failures);
            out.writeInt(counts.invalid);
        }
    }

    public static NodeImportResponse readNew(StreamInput in) throws IOException {
        NodeImportResponse response = new NodeImportResponse();
        response.readFrom(in);
        return response;
    }

    static final class Fields {
        static final XContentBuilderString NODE_ID = new XContentBuilderString("node_id");
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString IMPORTED_FILES = new XContentBuilderString("imported_files");
        static final XContentBuilderString FILE_NAME = new XContentBuilderString("file_name");
        static final XContentBuilderString SUCCESSES = new XContentBuilderString("successes");
        static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
        static final XContentBuilderString INVALIDATED = new XContentBuilderString("invalidated");
    }
}

package crate.elasticsearch.action.export;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Internal export response of a shard export request executed directly against a specific shard.
 */
class ShardExportResponse extends BroadcastShardOperationResponse implements ToXContent {

    private String stderr;
    private String stdout;
    private int exitCode;
    private List<String> cmdArray;
    private String cmd;
    private String file;
    private boolean dryRun = false;
    private Text node;
    private long numExported;

    ShardExportResponse() {
    }

    /**
     * Constructor for regular cases
     *
     * @param node        Name of the Node
     * @param index       Name of the index
     * @param shardId     ID of the shard
     * @param cmd         executed command (might be null)
     * @param cmdArray    executed command array (might be null)
     * @param file        written file (might be null)
     * @param stderr      output written to standard error by the executed command
     * @param stdout      output written to standard out by the executed command
     * @param exitCode    exit code of the executed command
     * @param numExported number of exported documents
     */
    public ShardExportResponse(Text node, String index, int shardId, String cmd, List<String> cmdArray, String file, String stderr, String stdout, int exitCode, long numExported) {
        super(index, shardId);
        this.node = node;
        this.cmd = cmd;
        this.cmdArray = cmdArray;
        this.file = file;
        this.stderr = stderr;
        this.stdout = stdout;
        this.exitCode = exitCode;
        this.numExported = numExported;
    }

    /**
     * Constructor for dry runs. Does not contain any execution infos
     *
     * @param node     Name of the Node
     * @param index    Name of the index
     * @param shardId  ID of the shard
     * @param cmd      executed command (might be null)
     * @param cmdArray executed command array (might be null)
     * @param file     written file (might be null)
     */
    public ShardExportResponse(Text node, String index, int shardId, String cmd, List<String> cmdArray, String file) {
        super(index, shardId);
        this.node = node;
        this.cmd = cmd;
        this.cmdArray = cmdArray;
        this.file = file;
        this.dryRun = true;
    }

    public String getCmd() {
        return cmd;
    }

    public List<String> getCmdArray() {
        return cmdArray;
    }

    public String getFile() {
        return file;
    }

    public String getStderr() {
        return stderr;
    }

    public String getStdout() {
        return stdout;
    }

    public int getExitCode() {
        return exitCode;
    }

    public long getNumExported() {
        return numExported;
    }


    public boolean dryRun() {
        return dryRun;
    }

    public Text getNode() {
        return node;
    }

    public static ShardExportResponse readNew(StreamInput in) throws IOException {
        ShardExportResponse response = new ShardExportResponse();
        response.readFrom(in);
        return response;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cmd = in.readOptionalString();
        cmdArray = new ArrayList<String>(Arrays.asList((String[]) in.readStringArray()));
        file = in.readOptionalString();
        stderr = in.readOptionalString();
        stdout = in.readOptionalString();
        exitCode = in.readVInt();
        numExported = in.readVLong();
        node = in.readOptionalText();
        dryRun = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(cmd);
        if (cmdArray == null) {
            out.writeStringArrayNullable(null);
        } else {
            out.writeStringArray(cmdArray.toArray(new String[cmdArray.size()]));
        }

        out.writeOptionalString(file);
        out.writeOptionalString(stderr);
        out.writeOptionalString(stdout);
        out.writeVInt(exitCode);
        out.writeVLong(numExported);
        out.writeOptionalText(node);
        out.writeBoolean(dryRun);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("index", getIndex());
        builder.field("shard", getShardId());
        if (node != null) {
            builder.field("node_id", node);
        }
        builder.field("numExported", getNumExported());
        if (getFile() != null) {
            builder.field("output_file", getFile());
        } else {
            builder.field("output_cmd", getCmd() != null ? getCmd() : getCmdArray());
            if (!dryRun()) {
                builder.field("stderr", getStderr());
                builder.field("stdout", getStdout());
                builder.field("exitcode", getExitCode());
            }
        }
        builder.endObject();
        return builder;
    }
}

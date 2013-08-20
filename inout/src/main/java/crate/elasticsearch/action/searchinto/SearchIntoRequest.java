package crate.elasticsearch.action.searchinto;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Required;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.Arrays;


public class SearchIntoRequest extends
        BroadcastOperationRequest<SearchIntoRequest> {

    @Nullable
    protected String routing;

    @Nullable
    private String preference;

    private BytesReference source;
    private boolean querySourceUnsafe;

    private String[] types = Strings.EMPTY_ARRAY;

    SearchIntoRequest() {
    }

    /**
     * Constructs a new inout request against the provided indices. No
     * indices provided means it will
     * run against all indices.
     */
    public SearchIntoRequest(String... indices) {
        super(indices);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super
                .validate();
        return validationException;
    }

    @Override
    protected void beforeStart() {
        if (querySourceUnsafe) {
            source = source.copyBytesArray();
            querySourceUnsafe = false;
        }
    }

    /**
     * The query source to execute.
     */
    public BytesReference source() {
        return source;
    }

    @Required
    public SearchIntoRequest source(String source) {
        return this.source(new BytesArray(source), false);
    }


    @Required
    public SearchIntoRequest source(BytesReference source, boolean unsafe) {
        this.source = source;
        this.querySourceUnsafe = unsafe;
        return this;
    }

    /**
     * The types of documents the query will run against. Defaults to all
     * types.
     */
    String[] types() {
        return this.types;
    }

    /**
     * The types of documents the query will run against. Defaults to all
     * types.
     */
    public SearchIntoRequest types(String... types) {
        this.types = types;
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the
     * search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the
     * search will be executed on.
     */
    public SearchIntoRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The routing values to control the shards that the search will be
     * executed on.
     */
    public SearchIntoRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    public SearchIntoRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        querySourceUnsafe = false;
        source = in.readBytesReference();
        types = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeBytesReference(source);
        out.writeStringArray(types);
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(source, false);
        } catch (Exception e) {
            // ignore
        }
        return "[" + Arrays.toString(indices) + "]" + Arrays.toString(
                types) + ", querySource[" + sSource + "]";
    }
}

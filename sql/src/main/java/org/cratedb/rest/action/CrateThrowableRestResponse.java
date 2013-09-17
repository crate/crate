package org.cratedb.rest.action;

import org.cratedb.sql.CrateException;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;

import java.io.IOException;

import static org.elasticsearch.ExceptionsHelper.detailedMessage;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;


public class CrateThrowableRestResponse extends XContentRestResponse {

    public CrateThrowableRestResponse(RestRequest request, Throwable t) throws IOException {
        this(request, ((t instanceof ElasticSearchException) ? ((ElasticSearchException) t).status() : RestStatus.INTERNAL_SERVER_ERROR), t);
    }

    public CrateThrowableRestResponse(RestRequest request, RestStatus status, Throwable t) throws IOException {
        super(request, status, convert(request, status, t));
    }

    private static XContentBuilder convert(RestRequest request, RestStatus status, Throwable t) throws IOException {
        XContentBuilder builder = restContentBuilder(request).startObject()
            .startObject("error");

        builder.field("message", detailedMessage(t));
        if (t instanceof CrateException) {
            CrateException cex = (CrateException)t;
            builder.field("code", cex.errorCode());
            if (cex.args().length > 0) {
                builder.field("args", cex.args());
            }
        } else {
            builder.field("code", 1000);
        }

        builder.endObject();

        if (t != null && request.paramAsBoolean("error_trace", false)) {
            builder.startObject("error_trace");
            boolean first = true;
            while (t != null) {
                if (!first) {
                    builder.startObject("cause");
                }
                buildThrowable(t, builder);
                if (!first) {
                    builder.endObject();
                }
                t = t.getCause();
                first = false;
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private static void buildThrowable(Throwable t, XContentBuilder builder) throws IOException {
        builder.field("message", t.getMessage());
        for (StackTraceElement stElement : t.getStackTrace()) {
            builder.startObject("at")
                    .field("class", stElement.getClassName())
                    .field("method", stElement.getMethodName());
            if (stElement.getFileName() != null) {
                builder.field("file", stElement.getFileName());
            }
            if (stElement.getLineNumber() >= 0) {
                builder.field("line", stElement.getLineNumber());
            }
            builder.endObject();
        }
    }
}



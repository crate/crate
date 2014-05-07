/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.rest.action;

import io.crate.action.sql.SQLActionException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;

import java.io.IOException;

import static org.elasticsearch.ExceptionsHelper.detailedMessage;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;


public class CrateThrowableRestResponse extends XContentRestResponse {

    public CrateThrowableRestResponse(RestRequest request, Throwable t) throws IOException {
        this(request, ((t instanceof ElasticsearchException) ? ((ElasticsearchException) t).status() : RestStatus.INTERNAL_SERVER_ERROR), t);
    }

    public CrateThrowableRestResponse(RestRequest request, RestStatus status, Throwable t) throws IOException {
        super(request, status, convert(request, t));
    }

    private static XContentBuilder convert(RestRequest request, Throwable t) throws IOException {
        XContentBuilder builder = restContentBuilder(request).startObject()
            .startObject("error");

        SQLActionException sqlActionException = null;
        builder.field("message", detailedMessage(t));
        if (t instanceof SQLActionException) {
            sqlActionException = (SQLActionException)t;
            builder.field("code", sqlActionException.errorCode());
        } else {
            builder.field("code", 5000);
        }

        builder.endObject();

        if (t != null && request.paramAsBoolean("error_trace", false)
                && sqlActionException != null) {
            builder.field("error_trace", sqlActionException.stackTrace());
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



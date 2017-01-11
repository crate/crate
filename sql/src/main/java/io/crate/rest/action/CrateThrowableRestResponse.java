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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.elasticsearch.ExceptionsHelper.detailedMessage;


class CrateThrowableRestResponse extends RestResponse {

    private final RestStatus status;
    private final BytesReference content;
    private final String contentType;

    CrateThrowableRestResponse(RestChannel channel, Throwable t) throws IOException {
        status = (t instanceof ElasticsearchException) ?
            ((ElasticsearchException) t).status() :
            RestStatus.INTERNAL_SERVER_ERROR;
        if (channel.request().method() == RestRequest.Method.HEAD) {
            this.content = BytesArray.EMPTY;
            this.contentType = BytesRestResponse.TEXT_CONTENT_TYPE;
        } else {
            XContentBuilder builder = convert(channel, t);
            this.content = builder.bytes();
            this.contentType = builder.contentType().mediaType();
        }
    }

    private static XContentBuilder convert(RestChannel channel, Throwable t) throws IOException {
        XContentBuilder builder = channel.newBuilder().startObject()
            .startObject("error");

        SQLActionException sqlActionException = null;
        builder.field("message", detailedMessage(t));
        if (t instanceof SQLActionException) {
            sqlActionException = (SQLActionException) t;
            builder.field("code", sqlActionException.errorCode());
        } else {
            builder.field("code", 5000);
        }

        builder.endObject();

        if (t != null && channel.request().paramAsBoolean("error_trace", false)
            && sqlActionException != null) {
            StringWriter stackTrace = new StringWriter();
            sqlActionException.printStackTrace(new PrintWriter(stackTrace));
            builder.field("error_trace", stackTrace.toString());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String contentType() {
        return contentType;
    }

    @Override
    public BytesReference content() {
        return content;
    }

    @Override
    public RestStatus status() {
        return status;
    }
}



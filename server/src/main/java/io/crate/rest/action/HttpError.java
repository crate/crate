/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.rest.action;

import io.crate.auth.user.AccessControl;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;

import static io.crate.exceptions.Exceptions.userFriendlyMessageInclNested;

public class HttpError {

    private final HttpResponseStatus status;
    private final Throwable t;
    private final int errorCode;

    public HttpError(HttpResponseStatus status, int errorCode, Throwable t) {
        this.status = status;
        this.errorCode = errorCode;
        this.t = t;
    }

    static HttpError convert(AccessControl accessControl, Throwable t) {
        return new HttpError(HttpResponseStatus.INTERNAL_SERVER_ERROR, 5000, t);
    }

    public XContentBuilder toXContent(boolean includeErrorTrace) throws IOException {
        // @formatter:off
        XContentBuilder builder = JsonXContent.contentBuilder()
            .startObject()
            .startObject("error")
            .field("message", userFriendlyMessageInclNested(t))
            .field("code", errorCode)
            .endObject();
        // @formatter:on

        if (includeErrorTrace) {
            builder.field("error_trace", ExceptionsHelper.stackTrace(t));
        }
        return builder.endObject();
    }

    public HttpResponseStatus status() {
        return status;
    }

}

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

import io.crate.action.sql.SQLActionException;
import io.crate.auth.user.AccessControl;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SQLExceptions;
import io.crate.exceptions.SQLParseException;
import io.crate.exceptions.UnauthorizedException;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import javax.annotation.Nullable;
import java.io.IOException;

import static io.crate.exceptions.Exceptions.userFriendlyMessageInclNested;

public class HttpError {

    private final HttpResponseStatus httpStatus;
    private final CrateErrorStatus status;
    private final String message;

    @Nullable
    private final Throwable t;

    public HttpError(HttpResponseStatus httpStatus, CrateErrorStatus status, String message, @Nullable Throwable t) {
        this.httpStatus = httpStatus;
        this.status = status;
        this.message = message;
        this.t = t;
    }

    public CrateErrorStatus status() {
        return status;
    }

    public String message() {
        return message;
    }

    public HttpResponseStatus httpStatus() {
        return httpStatus;
    }

    public XContentBuilder toXContent(boolean includeErrorTrace) throws IOException {
        // @formatter:off
        XContentBuilder builder = JsonXContent.contentBuilder()
            .startObject()
            .startObject("error")
            .field("message", userFriendlyMessageInclNested(t))
            .field("code", status.errorCode())
            .endObject();
        // @formatter:on

        if (includeErrorTrace) {
            builder.field("error_trace", ExceptionsHelper.stackTrace(t));
        }
        return builder.endObject();
    }

    @Override
    public String toString() {
        return "HttpError{" +
               "httpStatus=" + httpStatus +
               ", status=" + status +
               ", message='" + message + '\'' +
               ", t=" + t +
               '}';
    }

    public static HttpError fromThrowable(Throwable throwable, @Nullable AccessControl accessControl) {
        Throwable unwrappedError = SQLExceptions.unwrap(throwable);
        //TODO make sure values are masked using accessControl
        HttpResponseStatus httpStatus;
        CrateErrorStatus crateErrorStatus;


        if (unwrappedError instanceof IllegalArgumentException) {
            httpStatus = HttpResponseStatus.BAD_REQUEST;
            crateErrorStatus = CrateErrorStatus.USER_NOT_AUTHORIZED_TO_PERFORM_STATEMENT;
        } else if (unwrappedError instanceof UnauthorizedException) {
            httpStatus = HttpResponseStatus.UNAUTHORIZED;
            crateErrorStatus = CrateErrorStatus.USER_NOT_AUTHORIZED_TO_PERFORM_STATEMENT;
        } else if (unwrappedError instanceof SQLActionException) {
            httpStatus = HttpResponseStatus.BAD_REQUEST;
            crateErrorStatus = CrateErrorStatus.STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX;
        } else if (unwrappedError instanceof SQLParseException) {
            httpStatus = HttpResponseStatus.BAD_REQUEST;
            crateErrorStatus = CrateErrorStatus.STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX;
        } else if (unwrappedError instanceof RelationUnknown) {
            httpStatus = HttpResponseStatus.NOT_FOUND;
            crateErrorStatus = CrateErrorStatus.UNKNOWN_RELATION;
        } else {
            httpStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            crateErrorStatus = CrateErrorStatus.UNHANDLED_SERVER_ERROR;
        }
        return new HttpError(httpStatus, crateErrorStatus, unwrappedError.getMessage(), unwrappedError);
    }
}

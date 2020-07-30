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

import io.crate.exceptions.CrateException;
import io.crate.exceptions.DuplicateKeyException;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SQLExceptions;
import io.crate.exceptions.SQLParseException;
import io.crate.exceptions.UnauthorizedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Locale;

import static io.crate.exceptions.Exceptions.userFriendlyMessageInclNested;

public class HttpError {

    private final HttpErrorStatus status;
    private final String message;

    @Nullable
    private final Throwable t;

    public HttpError(HttpErrorStatus status, String message, @Nullable Throwable t) {
        this.status = status;
        this.message = message;
        this.t = t;
    }

    public HttpErrorStatus status() {
        return status;
    }

    public String message() {
        return message;
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
               ", status=" + status +
               ", message='" + message + '\'' +
               ", t=" + t +
               '}';
    }

    public static HttpError fromThrowable(Throwable throwable) {
        Throwable unwrappedError = SQLExceptions.handleException(throwable, null);
        HttpErrorStatus httpErrorStatus;
        if (unwrappedError instanceof HttpResponseException) {
            httpErrorStatus = ((HttpResponseException) unwrappedError).status();
        } else if (unwrappedError instanceof IllegalArgumentException) {
            httpErrorStatus = HttpErrorStatus.USER_NOT_AUTHORIZED_TO_PERFORM_STATEMENT;
        } else if (unwrappedError instanceof UnauthorizedException) {
            httpErrorStatus = HttpErrorStatus.USER_NOT_AUTHORIZED_TO_PERFORM_STATEMENT;
        } else if (unwrappedError instanceof SQLParseException) {
            httpErrorStatus = HttpErrorStatus.STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX;
        } else if (unwrappedError instanceof RelationUnknown) {
            httpErrorStatus = HttpErrorStatus.UNKNOWN_RELATION;
        } else if (unwrappedError instanceof RelationAlreadyExists) {
            httpErrorStatus = HttpErrorStatus.RELATION_WITH_THE_SAME_NAME_EXISTS_ALREADY;
        } else if (unwrappedError instanceof DuplicateKeyException) {
            httpErrorStatus = HttpErrorStatus.DOCUMENT_WITH_THE_SAME_PRIMARY_KEY_EXISTS_ALREADY;
        } else {
            httpErrorStatus = HttpErrorStatus.UNHANDLED_SERVER_ERROR;
        }
        String message = unwrappedError.getMessage();
        if (message == null) {
            if (throwable instanceof CrateException && throwable.getCause() != null) {
                throwable = throwable.getCause();   // use cause because it contains a more meaningful error in most cases
            }
            StackTraceElement[] stackTraceElements = throwable.getStackTrace();
            if (stackTraceElements.length > 0) {
                message = String.format(Locale.ENGLISH,
                                        "%s in %s",
                                        throwable.getClass().getSimpleName(),
                                        stackTraceElements[0]);
            } else {
                message = "Error in " + throwable.getClass().getSimpleName();
            }
        }
        return new HttpError(httpErrorStatus, message, unwrappedError);
    }
}

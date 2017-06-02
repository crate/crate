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

import io.crate.action.sql.BaseResultReceiver;
import io.crate.analyze.symbol.Field;
import io.crate.data.Row;
import io.crate.operation.user.ExceptionAuthorizedValidator;
import io.crate.exceptions.SQLExceptions;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;

class RestRowCountReceiver extends BaseResultReceiver {

    private static final Logger LOGGER = Loggers.getLogger(RestRowCountReceiver.class);

    private final RestChannel channel;
    private final ExceptionAuthorizedValidator exceptionAuthorizedValidator;
    private final long startTime;
    private final boolean includeTypes;
    private long rowCount;
    private final ResultToXContentBuilder builder;

    RestRowCountReceiver(RestChannel channel,
                         ExceptionAuthorizedValidator exceptionAuthorizedValidator,
                         long startTime,
                         boolean includeTypes) {
        this.channel = channel;
        this.exceptionAuthorizedValidator = exceptionAuthorizedValidator;
        this.startTime = startTime;
        this.includeTypes = includeTypes;
        builder = builder();
        assert builder != null : "builder should not be null";
    }

    private ResultToXContentBuilder builder() {
        ResultToXContentBuilder builder = null;
        try {
            builder = ResultToXContentBuilder.builder(channel);
        } catch (IOException e) {
            fail(e);
        }
        return builder;
    }

    @Override
    public void setNextRow(Row row) {
        rowCount = (long) row.get(0);
    }

    XContentBuilder finishBuilder() throws IOException {
        builder.cols(Collections.<Field>emptyList());
        if (includeTypes) {
            builder.colTypes(Collections.<Field>emptyList());
        }
        builder.startRows()
            .addRow(Row.EMPTY, 0)
            .finishRows()
            .rowCount(rowCount)
            .duration(startTime);
        return builder.build();
    }

    @Override
    public void allFinished(boolean interrupted) {
        try {
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, finishBuilder()));
            super.allFinished(interrupted);
        } catch (Throwable e) {
            fail(e);
        }
    }

    @Override
    public void fail(@Nonnull Throwable t) {
        try {
            channel.sendResponse(new CrateThrowableRestResponse(channel,
                SQLExceptions.createSQLActionException(t, exceptionAuthorizedValidator)));
        } catch (Throwable e) {
            LOGGER.error("failed to send failure response", e);
        } finally {
            super.fail(t);
        }
    }
}

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
import io.crate.expression.symbol.Field;
import io.crate.breaker.RowAccounting;
import io.crate.data.Row;
import io.crate.auth.user.ExceptionAuthorizedValidator;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static io.crate.exceptions.SQLExceptions.createSQLActionException;

class RestResultSetReceiver extends BaseResultReceiver {

    private static final Logger LOGGER = Loggers.getLogger(RestResultSetReceiver.class);

    private final RestChannel channel;
    private final ExceptionAuthorizedValidator exceptionAuthorizedValidator;
    private final List<Field> outputFields;
    private final ResultToXContentBuilder builder;
    private long startTime;
    private final RowAccounting rowAccounting;
    private long rowCount;

    RestResultSetReceiver(RestChannel channel,
                          ExceptionAuthorizedValidator exceptionAuthorizedValidator,
                          List<Field> outputFields,
                          long startTime,
                          RowAccounting rowAccounting,
                          boolean includeTypesOnResponse) {
        this.channel = channel;
        this.exceptionAuthorizedValidator = exceptionAuthorizedValidator;
        this.outputFields = outputFields;
        this.startTime = startTime;
        this.rowAccounting = rowAccounting;
        ResultToXContentBuilder tmpBuilder;
        try {
            tmpBuilder = ResultToXContentBuilder.builder(channel);
            tmpBuilder.cols(outputFields);
            if (includeTypesOnResponse) {
                tmpBuilder.colTypes(outputFields);
            }
            tmpBuilder.startRows();
        } catch (IOException e) {
            tmpBuilder = null;
            fail(e);
        }
        assert tmpBuilder != null : "tmpBuilder must not be null";
        builder = tmpBuilder;
    }

    @Override
    public void setNextRow(Row row) {
        try {
            rowAccounting.accountForAndMaybeBreak(row);
            builder.addRow(row, outputFields.size());
            rowCount++;
        } catch (IOException e) {
            fail(e);
        }
    }

    @Override
    public void allFinished(boolean interrupted) {
        BytesRestResponse response;
        try {
            response = new BytesRestResponse(RestStatus.OK, finishBuilder());
        } catch (Throwable t) {
            fail(t);
            return;
        }

        try {
            channel.sendResponse(response);
            super.allFinished(interrupted);
        } catch (Throwable e) {
            LOGGER.error("Failed to send final response.", e);
            super.fail(e);
        } finally {
            rowAccounting.close();
        }
    }

    @Override
    public void fail(@Nonnull Throwable t) {
        try {
            channel.sendResponse(new CrateThrowableRestResponse(channel,
                createSQLActionException(t, exceptionAuthorizedValidator)));
        } catch (Throwable e) {
            LOGGER.error("Failed to send error response for failed request.", e, t);
        } finally {
            rowAccounting.close();
            super.fail(t);
        }
    }

    XContentBuilder finishBuilder() throws IOException {
        return builder
            .finishRows()
            .rowCount(rowCount)
            .duration(startTime)
            .build();
    }
}

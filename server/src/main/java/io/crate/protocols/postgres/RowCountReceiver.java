/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.protocols.postgres;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.crate.action.sql.BaseResultReceiver;
import io.crate.auth.AccessControl;
import io.crate.data.Row;
import io.netty.channel.Channel;

class RowCountReceiver extends BaseResultReceiver {

    private static final Logger LOGGER = LogManager.getLogger(RowCountReceiver.class);
    private final Channel channel;
    private final String query;
    private final AccessControl accessControl;
    private long rowCount;

    RowCountReceiver(String query, Channel channel, AccessControl accessControl) {
        this.query = query;
        this.channel = channel;
        this.accessControl = accessControl;
    }

    @Override
    public void setNextRow(Row row) {
        rowCount = (long) row.get(0);
        /*
         * In Crate -1 means row-count unknown, and -2 means error. In JDBC -2 means row-count unknown and -3 means error.
         * See {@link java.sql.Statement#EXECUTE_FAILED}
         */
        if (rowCount < 0) {
            rowCount--;
        }
    }

    @Override
    public void allFinished(boolean interrupted) {
        LOGGER.debug("query={} finished -> sending commandComplete", query);
        Messages.sendCommandComplete(channel, query, rowCount).addListener(f -> super.allFinished(interrupted));
    }

    @Override
    public void fail(@Nonnull Throwable throwable) {
        Messages.sendErrorResponse(channel, accessControl, throwable).addListener(f -> super.fail(throwable));
    }
}

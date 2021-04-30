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

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.crate.action.sql.BaseResultReceiver;
import io.crate.auth.AccessControl;
import io.crate.data.Row;
import io.crate.protocols.postgres.types.PGType;
import io.netty.channel.Channel;

class ResultSetReceiver extends BaseResultReceiver {

    private final String query;
    private final Channel channel;
    private final List<PGType<?>> columnTypes;
    private final TransactionState transactionState;
    private final AccessControl accessControl;

    @Nullable
    private final FormatCodes.FormatCode[] formatCodes;

    private long rowCount = 0;

    ResultSetReceiver(String query,
                      Channel channel,
                      TransactionState transactionState,
                      AccessControl accessControl,
                      List<PGType<?>> columnTypes,
                      @Nullable FormatCodes.FormatCode[] formatCodes) {
        this.query = query;
        this.channel = channel;
        this.transactionState = transactionState;
        this.accessControl = accessControl;
        this.columnTypes = columnTypes;
        this.formatCodes = formatCodes;
    }

    @Override
    public void setNextRow(Row row) {
        rowCount++;
        Messages.sendDataRow(channel, row, columnTypes, formatCodes);
        if (rowCount % 1000 == 0) {
            channel.flush();
        }
    }

    @Override
    public void batchFinished() {
        Messages.sendPortalSuspended(channel);
        Messages.sendReadyForQuery(channel, transactionState);
    }

    @Override
    public void allFinished(boolean interrupted) {
        if (interrupted) {
            super.allFinished(true);
        } else {
            Messages.sendCommandComplete(channel, query, rowCount).addListener(f -> super.allFinished(false));
        }
    }

    @Override
    public void fail(@Nonnull Throwable throwable) {
        Messages.sendErrorResponse(channel, accessControl, throwable).addListener(f -> super.fail(throwable));
    }
}

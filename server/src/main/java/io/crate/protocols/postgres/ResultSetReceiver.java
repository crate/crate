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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.session.BaseResultReceiver;
import io.crate.auth.AccessControl;
import io.crate.data.Row;
import io.crate.protocols.postgres.DelayableWriteChannel.DelayedWrites;
import io.crate.protocols.postgres.types.PGType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

class ResultSetReceiver extends BaseResultReceiver {

    private final String query;
    private final DelayableWriteChannel channel;
    private final List<PGType<?>> columnTypes;
    private final TransactionState transactionState;
    private final AccessControl accessControl;
    private final Channel directChannel;
    private final DelayedWrites delayedWrites;

    @Nullable
    private final FormatCodes.FormatCode[] formatCodes;

    private long rowCount = 0;

    ResultSetReceiver(String query,
                      DelayableWriteChannel channel,
                      DelayedWrites delayedWrites,
                      TransactionState transactionState,
                      AccessControl accessControl,
                      List<PGType<?>> columnTypes,
                      @Nullable FormatCodes.FormatCode[] formatCodes) {
        this.query = query;
        this.channel = channel;
        this.delayedWrites = delayedWrites;
        this.directChannel = channel.bypassDelay();
        this.transactionState = transactionState;
        this.accessControl = accessControl;
        this.columnTypes = columnTypes;
        this.formatCodes = formatCodes;
    }

    @Override
    public void setNextRow(Row row) {
        rowCount++;
        Messages.sendDataRow(directChannel, row, columnTypes, formatCodes);
        if (rowCount % 1000 == 0) {
            directChannel.flush();
        }
    }

    @Override
    public void batchFinished() {
        ChannelFuture sendPortalSuspended = Messages.sendPortalSuspended(directChannel);
        channel.writePendingMessages(delayedWrites);
        channel.flush();

        // Trigger the completion future but by-pass `sendCompleteComplete`
        // This resultReceiver shouldn't be used anymore. The next `execute` message
        // from the client will create a new one.
        sendPortalSuspended.addListener(f -> super.allFinished());
    }

    @Override
    public void allFinished() {
        ChannelFuture sendCommandComplete = Messages.sendCommandComplete(directChannel, query, rowCount);
        channel.writePendingMessages(delayedWrites);
        channel.flush();
        sendCommandComplete.addListener(f -> super.allFinished());
    }

    @Override
    public void fail(@NotNull Throwable throwable) {
        ChannelFuture sendErrorResponse = Messages.sendErrorResponse(directChannel, accessControl, throwable);
        channel.writePendingMessages(delayedWrites);
        channel.flush();
        sendErrorResponse.addListener(f -> super.fail(throwable));
    }
}

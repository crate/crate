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
import java.util.concurrent.CompletableFuture;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.auth.AccessControl;
import io.crate.data.Row;
import io.crate.protocols.postgres.DelayableWriteChannel.DelayedWrites;
import io.crate.protocols.postgres.types.PGType;
import io.crate.session.BaseResultReceiver;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

class ResultSetReceiver extends BaseResultReceiver {

    private final String query;
    private final DelayableWriteChannel channel;
    private final List<PGType<?>> columnTypes;
    private final AccessControl accessControl;
    private final Channel directChannel;
    private final DelayedWrites delayedWrites;

    @Nullable
    private final FormatCodes.FormatCode[] formatCodes;

    private long rowCount = 0;

    ResultSetReceiver(String query,
                      DelayableWriteChannel channel,
                      DelayedWrites delayedWrites,
                      AccessControl accessControl,
                      List<PGType<?>> columnTypes,
                      @Nullable FormatCodes.FormatCode[] formatCodes) {
        this.query = query;
        this.channel = channel;
        this.delayedWrites = delayedWrites;
        this.directChannel = channel.bypassDelay();
        this.accessControl = accessControl;
        this.columnTypes = columnTypes;
        this.formatCodes = formatCodes;
    }

    /**
     * Writes the row to the pg channel and flushes the channel if necessary.
     *
     * @return a future that is completed once the row was successfully written (flushed)
     */
    @Override
    @Nullable
    public CompletableFuture<Void> setNextRow(Row row) {
        rowCount++;
        ChannelFuture sendDataRow = Messages.sendDataRow(directChannel, row, columnTypes, formatCodes);
        CompletableFuture<Void> future;
        boolean isWritable = directChannel.isWritable();
        if (isWritable) {
            future = null;
        } else {
            future = new CompletableFuture<>();
            sendDataRow.addListener(f -> {
                if (f.isDone() == false) {
                    return;
                }
                if (f.isSuccess()) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(f.cause());
                }
            });
        }

        // Flush the channel only every 1000 rows for better performance.
        // But flushing must be forced once the channel outbound buffer is full (= channel not in writable state)
        if (isWritable == false || rowCount % 1000 == 0) {
            directChannel.flush();
        }
        return future;
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

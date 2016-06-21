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

package io.crate.protocols.postgres;

import io.crate.core.collections.Row;
import io.crate.exceptions.Exceptions;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.Requirements;
import io.crate.operation.projectors.RowReceiver;
import org.jboss.netty.channel.Channel;

import java.util.Set;

class RowCountReceiver implements RowReceiver {

    private final Channel channel;
    private final String query;
    private long rowCount;

    RowCountReceiver(String query, Channel channel) {
        this.query = query;
        this.channel = channel;
    }

    @Override
    public boolean setNextRow(Row row) {
        rowCount = (long) row.get(0);
        return true;
    }

    @Override
    public void finish() {
        Messages.sendCommandComplete(channel, query, rowCount);
        Messages.sendReadyForQuery(channel);
    }

    @Override
    public void fail(Throwable throwable) {
        Messages.sendErrorResponse(channel, Exceptions.messageOf(throwable));
    }

    @Override
    public void kill(Throwable throwable) {
    }

    @Override
    public void prepare() {
    }

    @Override
    public void setUpstream(RowUpstream rowUpstream) {
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }
}

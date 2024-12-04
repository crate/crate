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

package io.crate.protocols;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

public final class ConnectionStats implements Writeable {

    private final long open;
    private final long total;

    private final long receivedMessages;

    private final long receivedBytes;

    private final long sentMessages;

    private final long sentBytes;

    public ConnectionStats(long open,
                           long total,
                           long receivedMessages,
                           long receivedBytes,
                           long sentMessages,
                           long sentBytes) {
        this.open = open;
        this.total = total;
        this.receivedMessages = receivedMessages;
        this.receivedBytes = receivedBytes;
        this.sentMessages = sentMessages;
        this.sentBytes = sentBytes;
    }

    public ConnectionStats(StreamInput in) throws IOException {
        this.open = in.readVLong();
        this.total = in.readVLong();
        this.receivedMessages = in.readVLong();
        this.receivedBytes = in.readVLong();
        this.sentMessages = in.readVLong();
        this.sentBytes = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(open);
        out.writeVLong(total);
        out.writeVLong(receivedMessages);
        out.writeVLong(receivedBytes);
        out.writeVLong(sentMessages);
        out.writeVLong(sentBytes);
    }

    public long open() {
        return open;
    }

    public long total() {
        return total;
    }

    public long receivedMsgs() {
        return receivedMessages;
    }

    public long receivedBytes() {
        return receivedBytes;
    }

    public long sentMsgs() {
        return sentMessages;
    }

    public long sentBytes() {
        return sentBytes;
    }
}

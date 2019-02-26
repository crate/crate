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

package io.crate.es.transport;

import io.crate.es.common.io.stream.StreamInput;
import io.crate.es.common.io.stream.StreamOutput;
import io.crate.es.common.transport.TransportAddress;

import java.io.IOException;

/**
 * An action invocation failure.
 *
 *
 */
public class ActionTransportException extends TransportException {

    private final TransportAddress address;

    private final String action;

    public ActionTransportException(StreamInput in) throws IOException {
        super(in);
        address = in.readOptionalWriteable(TransportAddress::new);
        action = in.readOptionalString();
    }

    public ActionTransportException(String name, TransportAddress address, String action, Throwable cause) {
        super(buildMessage(name, address, action, null), cause);
        this.address = address;
        this.action = action;
    }

    public ActionTransportException(String name, TransportAddress address, String action, String msg, Throwable cause) {
        super(buildMessage(name, address, action, msg), cause);
        this.address = address;
        this.action = action;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(address);
        out.writeOptionalString(action);
    }

    /**
     * The target address to invoke the action on.
     */
    public TransportAddress address() {
        return address;
    }

    /**
     * The action to invoke.
     */
    public String action() {
        return action;
    }

    private static String buildMessage(String name, TransportAddress address, String action, String msg) {
        StringBuilder sb = new StringBuilder();
        if (name != null) {
            sb.append('[').append(name).append(']');
        }
        if (address != null) {
            sb.append('[').append(address).append(']');
        }
        if (action != null) {
            sb.append('[').append(action).append(']');
        }
        if (msg != null) {
            sb.append(" ").append(msg);
        }
        return sb.toString();
    }
}

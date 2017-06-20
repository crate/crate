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

import io.crate.operation.auth.Protocol;
import io.netty.handler.ssl.SslHandler;

import javax.annotation.Nullable;
import java.net.InetAddress;

public class ConnectionProperties {

    private final InetAddress address;
    private final Protocol protocol;
    private final boolean hasSSL;

    public ConnectionProperties(InetAddress address, Protocol protocol, @Nullable SslHandler sslHandler) {
        this.address = address;
        this.protocol = protocol;
        this.hasSSL = sslHandler != null;
    }

    public boolean hasSSL() {
        return hasSSL;
    }

    public InetAddress address() {
        return address;
    }

    public Protocol protocol() {
        return protocol;
    }
}

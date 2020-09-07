/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;

import io.crate.concurrent.CompletableContext;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

public class FakeTcpChannel implements TcpChannel {

    private final boolean isServer;
    private final String profile;
    private final ChannelStats stats = new ChannelStats();
    private final CompletableContext<Void> closeContext = new CompletableContext<>();
    private final AtomicReference<BytesReference> messageCaptor;
    private final AtomicReference<ActionListener<Void>> listenerCaptor;

    public FakeTcpChannel() {
        this(false, "profile", new AtomicReference<>());
    }

    public FakeTcpChannel(boolean isServer) {
        this(isServer, "profile", new AtomicReference<>());
    }

    public FakeTcpChannel(boolean isServer, AtomicReference<BytesReference> messageCaptor) {
        this(isServer, "profile", messageCaptor);
    }


    public FakeTcpChannel(boolean isServer, String profile, AtomicReference<BytesReference> messageCaptor) {
        this.isServer = isServer;
        this.profile = profile;
        this.messageCaptor = messageCaptor;
        this.listenerCaptor = new AtomicReference<>();
    }

    @Override
    public boolean isServerChannel() {
        return isServer;
    }

    @Override
    public String getProfile() {
        return profile;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return null;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return null;
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        messageCaptor.set(reference);
        listenerCaptor.set(listener);
    }

    @Override
    public void addConnectListener(ActionListener<Void> listener) {

    }

    @Override
    public void close() {
        closeContext.complete(null);
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public boolean isOpen() {
        return closeContext.isDone() == false;
    }

    @Override
    public ChannelStats getChannelStats() {
        return stats;
    }

    public AtomicReference<BytesReference> getMessageCaptor() {
        return messageCaptor;
    }

    public AtomicReference<ActionListener<Void>> getListenerCaptor() {
        return listenerCaptor;
    }
}

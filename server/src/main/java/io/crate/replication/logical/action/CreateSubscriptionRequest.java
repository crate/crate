/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.action;

import io.crate.replication.logical.metadata.ConnectionInfo;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.List;

public class CreateSubscriptionRequest extends AcknowledgedRequest<CreateSubscriptionRequest> {

    private final String owner;
    private final String name;
    private final ConnectionInfo connectionInfo;
    private final List<String> publications;
    private final Settings settings;

    public CreateSubscriptionRequest(String owner,
                                     String name,
                                     ConnectionInfo connectionInfo,
                                     List<String> publications,
                                     Settings settings) {
        this.owner = owner;
        this.name = name;
        this.connectionInfo = connectionInfo;
        this.publications = publications;
        this.settings = settings;
    }

    public CreateSubscriptionRequest(StreamInput in) throws IOException {
        super(in);
        this.owner = in.readString();
        this.name = in.readString();
        connectionInfo = new ConnectionInfo(in);
        this.publications = List.of(in.readStringArray());
        settings = Settings.readSettingsFromStream(in);
    }

    public String owner() {
        return owner;
    }

    public String name() {
        return name;
    }

    public ConnectionInfo connectionInfo() {
        return connectionInfo;
    }

    public List<String> publications() {
        return publications;
    }

    public Settings settings() {
        return settings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(owner);
        out.writeString(name);
        connectionInfo.writeTo(out);
        out.writeStringArray(publications.toArray(new String[0]));
        Settings.writeSettingsToStream(settings, out);
    }
}

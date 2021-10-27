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

package io.crate.replication.logical.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Subscription implements Writeable {

    private final String owner;
    private final ConnectionInfo connectionInfo;
    private final List<String> publications;
    private final Settings settings;

    public Subscription(String owner,
                        ConnectionInfo connectionInfo,
                        List<String> publications,
                        Settings settings) {
        this.owner = owner;
        this.connectionInfo = connectionInfo;
        this.publications = publications;
        this.settings = settings;
    }

    Subscription(StreamInput in) throws IOException {
        owner = in.readString();
        connectionInfo = new ConnectionInfo(in);
        publications = Arrays.stream(in.readStringArray()).toList();
        settings = Settings.readSettingsFromStream(in);
    }

    public String owner() {
        return owner;
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
        out.writeString(owner);
        connectionInfo.writeTo(out);
        out.writeStringArray(publications.toArray(new String[0]));
        Settings.writeSettingsToStream(settings, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Subscription that = (Subscription) o;
        return owner.equals(that.owner) && connectionInfo.equals(that.connectionInfo) &&
               publications.equals(that.publications) && settings.equals(that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(owner, connectionInfo, publications, settings);
    }
}

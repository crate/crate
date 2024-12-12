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

package io.crate.fdw;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

public class AlterServerRequest extends AcknowledgedRequest<AlterServerRequest> {

    private final String name;
    private final Settings optionsAdded;
    private final Settings optionsUpdated;
    private final List<String> optionsRemoved;

    public AlterServerRequest(String name,
                              Settings optionsAdded,
                              Settings optionsUpdated,
                              List<String> optionsRemoved) {
        this.name = name;
        this.optionsAdded = optionsAdded;
        this.optionsUpdated = optionsUpdated;
        this.optionsRemoved = optionsRemoved;
    }

    public AlterServerRequest(StreamInput in) throws IOException {
        this.name = in.readString();
        this.optionsAdded = Settings.readSettingsFromStream(in);
        this.optionsUpdated = Settings.readSettingsFromStream(in);
        this.optionsRemoved = in.readList(StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        Settings.writeSettingsToStream(out, optionsAdded);
        Settings.writeSettingsToStream(out, optionsUpdated);
        out.writeCollection(optionsRemoved, StreamOutput::writeString);
    }

    public String name() {
        return name;
    }

    public Settings optionsAdded() {
        return optionsAdded;
    }

    public Settings optionsUpdated() {
        return optionsUpdated;
    }

    public List<String> optionsRemoved() {
        return optionsRemoved;
    }
}

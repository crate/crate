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

package io.crate.execution.ddl.tables;

import static org.elasticsearch.action.support.master.AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;

import java.io.IOException;

import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.RelationName;


public class CreateBlobTableRequest extends MasterNodeRequest<CreateBlobTableRequest> implements AckedRequest {

    private final RelationName name;
    private final Settings settings;

    public CreateBlobTableRequest(RelationName name, Settings settings) {
        this.name = name;
        this.settings = settings;
    }

    public CreateBlobTableRequest(StreamInput in) throws IOException {
        this.name = new RelationName(in);
        this.settings = Settings.readSettingsFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        name.writeTo(out);
        Settings.writeSettingsToStream(out, settings);
    }

    public RelationName name() {
        return name;
    }

    public Settings settings() {
        return settings;
    }

    @Override
    public TimeValue ackTimeout() {
        return DEFAULT_ACK_TIMEOUT;
    }
}

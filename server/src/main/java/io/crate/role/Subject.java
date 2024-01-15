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

package io.crate.role;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;

/**
 *
 * @param ident narrows the securable to a specific instance; null for Securable.CLUSTER
 */
public record Subject(Permission permission, Securable securable, @Nullable String ident)
        implements Writeable {

    Subject(StreamInput in) throws IOException {
        this(
            in.readEnum(Permission.class),
            in.readEnum(Securable.class),
            in.readOptionalString()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(permission);
        out.writeEnum(securable);
        out.writeOptionalString(ident);
    }
}

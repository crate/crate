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

package io.crate.metadata.view;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

public class ViewMetadata implements Writeable {

    private final String stmt;
    @Nullable
    private final String owner;

    ViewMetadata(String stmt, @Nullable String owner) {
        this.stmt = stmt;
        this.owner = owner;
    }

    ViewMetadata(StreamInput in) throws IOException {
        stmt = in.readString();
        owner = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(stmt);
        out.writeOptionalString(owner);
    }

    public String stmt() {
        return stmt;
    }

    @Nullable
    public String owner() {
        return owner;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ViewMetadata view = (ViewMetadata) o;
        return Objects.equals(stmt, view.stmt) &&
               Objects.equals(owner, view.owner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stmt, owner);
    }
}

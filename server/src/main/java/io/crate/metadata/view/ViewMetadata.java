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

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.SearchPath;

public record ViewMetadata(
        String stmt,
        @Nullable String owner,
        SearchPath searchPath) implements Writeable {

    public static ViewMetadata of(StreamInput in) throws IOException {
        String stmt = in.readString();
        String owner = in.readOptionalString();
        SearchPath searchPath;
        Version version = in.getVersion();
        if (version.onOrAfter(Version.V_5_3_5) && !version.equals(Version.V_5_4_0)) {
            searchPath = SearchPath.createSearchPathFrom(in);
        } else {
            searchPath = SearchPath.pathWithPGCatalogAndDoc();
        }
        return new ViewMetadata(stmt, owner, searchPath);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(stmt);
        out.writeOptionalString(owner);
        Version version = out.getVersion();
        if (version.onOrAfter(Version.V_5_3_5) && !version.equals(Version.V_5_4_0)) {
            searchPath.writeTo(out);
        }
    }
}

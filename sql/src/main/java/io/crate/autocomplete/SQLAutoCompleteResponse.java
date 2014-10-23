/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.autocomplete;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class SQLAutoCompleteResponse extends ActionResponse implements ToXContent {

    private int startIdx;
    private Collection<String> completions;

    public SQLAutoCompleteResponse() {}

    public SQLAutoCompleteResponse(int startIdx, Collection<String> completions) {
        this.startIdx = startIdx;
        this.completions = completions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("start_idx", startIdx);
        builder.startArray("completions");
        for (String completion : completions) {
            builder.value(completion);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(startIdx);
        out.writeVInt(completions.size());
        for (String completion : completions) {
            out.writeString(completion);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        startIdx = in.readVInt();
        int numCompletions = in.readVInt();
        completions = new ArrayList<>(numCompletions);
        for (int i = 0; i < numCompletions; i++) {
            completions.add(in.readString());
        }
    }
}

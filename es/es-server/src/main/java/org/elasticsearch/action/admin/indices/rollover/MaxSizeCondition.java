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

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * A size-based condition for an index size.
 * Evaluates to <code>true</code> if the index size is at least {@link #value}.
 */
public class MaxSizeCondition extends Condition<ByteSizeValue> {
    public static final String NAME = "max_size";

    public MaxSizeCondition(ByteSizeValue value) {
        super(NAME);
        this.value = value;
    }

    public MaxSizeCondition(StreamInput in) throws IOException {
        super(NAME);
        this.value = new ByteSizeValue(in.readVLong(), ByteSizeUnit.BYTES);
    }

    @Override
    public Result evaluate(Stats stats) {
        return new Result(this, stats.indexSize.getBytes() >= value.getBytes());
    }

    @Override
    boolean includedInVersion(Version version) {
        return version.onOrAfter(Version.V_6_1_0);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        //TODO here we should just use ByteSizeValue#writeTo and same for de-serialization in the constructor
        out.writeVLong(value.getBytes());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(NAME, value.getStringRep());
    }

    public static MaxSizeCondition fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
            return new MaxSizeCondition(ByteSizeValue.parseBytesSizeValue(parser.text(), NAME));
        } else {
            throw new IllegalArgumentException("invalid token: " + parser.currentToken());
        }
    }
}

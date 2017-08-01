/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.rule.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class IngestRule implements Writeable {

    private final String name;
    private final String targetTable;
    private final String condition;

    IngestRule(String name, String targetTable, String condition) {
        this.name = name;
        this.targetTable = targetTable;
        this.condition = condition;
    }

    IngestRule(StreamInput in) throws IOException {
        name = in.readString();
        targetTable = in.readString();
        condition = in.readString();
    }

    public String getName() {
        return name;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public String getCondition() {
        return condition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IngestRule that = (IngestRule) o;

        if (!name.equals(that.name)) return false;
        if (!targetTable.equals(that.targetTable)) return false;
        return condition.equals(that.condition);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + targetTable.hashCode();
        result = 31 * result + condition.hashCode();
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(targetTable);
        out.writeString(condition);
    }
}

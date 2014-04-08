/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.projection;

import com.google.common.collect.ImmutableList;
import io.crate.DataType;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Value;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WriterProjection extends Projection {

    private static final List<Symbol> OUTPUTS = ImmutableList.<Symbol>of(
            new Value(DataType.LONG) // number of lines written
    );
    private Symbol uri;

    @Nullable
    private Settings settings = ImmutableSettings.EMPTY;

    @Nullable
    private List<String> outputNames;

    public static final ProjectionFactory<WriterProjection> FACTORY = new ProjectionFactory<WriterProjection>() {
        @Override
        public WriterProjection newInstance() {
            return new WriterProjection();
        }
    };

    @Override
    public RowGranularity requiredGranularity() {
        return RowGranularity.SHARD;
    }

    public void outputNames(List<String> outputNames) {
        this.outputNames = outputNames;
    }

    @Nullable
    public List<String> outputNames() {
        return outputNames;
    }

    public Symbol uri() {
        return uri;
    }

    public void uri(Symbol uri) {
        this.uri = uri;
    }

    public void settings(Settings settings) {
        this.settings = settings;
    }

    public Settings settings() {
        return settings;
    }

    @Override
    public List<Symbol> outputs() {
        return OUTPUTS;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.WRITER;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitWriterProjection(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        uri = Symbol.fromStream(in);
        int size = in.readVInt();
        if (size > 0) {
            outputNames = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                outputNames.add(in.readString());
            }
        }
        settings = ImmutableSettings.readSettingsFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbol.toStream(uri, out);
        if (outputNames != null) {
            out.writeVInt(outputNames.size());
            for (String name : outputNames) {
                out.writeString(name);
            }
        } else {
            out.writeVInt(0);
        }
        ImmutableSettings.writeSettingsToStream(settings, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WriterProjection that = (WriterProjection) o;

        if (outputNames != null ? !outputNames.equals(that.outputNames) : that.outputNames != null) return false;
        if (settings != null ? !settings.equals(that.settings) : that.settings != null) return false;
        if (uri != null ? !uri.equals(that.uri) : that.uri != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + uri.hashCode();
        result = 31 * result + (outputNames != null ? outputNames.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WriterProjection{" +
                "uri=" + uri +
                ", settings=" + settings +
                ", outputNames=" + outputNames +
                '}';
    }

    @Override
    public WriterProjection normalize(EvaluatingNormalizer normalizer) {
        Symbol nUri = normalizer.normalize(uri);
        if (uri != nUri){
            WriterProjection p = new WriterProjection();
            p.uri = nUri;
            p.outputNames = outputNames;
            p.settings = settings;
            return p;
        }
        return this;
    }
}

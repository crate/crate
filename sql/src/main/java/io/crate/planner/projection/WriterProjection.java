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
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operation.scalar.FormatFunction;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WriterProjection extends Projection {

    private static final List<Symbol> OUTPUTS = ImmutableList.<Symbol>of(
            new Value(DataType.LONG) // number of lines written
    );

    private final static Reference SHARD_ID_REF = new Reference(SysShardsTableInfo.INFOS.get(new ColumnIdent("id")));
    private final static Reference TABLE_NAME_REF = new Reference(SysShardsTableInfo.INFOS.get(new ColumnIdent("table_name")));

    public static final Symbol DIRECTORY_TO_FILENAME = new Function(new FunctionInfo(
            new FunctionIdent(FormatFunction.NAME, Arrays.asList(DataType.STRING, DataType.STRING, DataType.STRING)),
            DataType.STRING),
            Arrays.<Symbol>asList(new StringLiteral("%s_%s.json"), TABLE_NAME_REF, SHARD_ID_REF)
    );

    private Symbol uri;
    private boolean isDirectoryUri = false;
    private List<Symbol> inputs = ImmutableList.of();

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

    public void isDirectoryUri(boolean isDirectoryUri) {
        this.isDirectoryUri = isDirectoryUri;
    }

    public boolean isDirectoryUri() {
        return isDirectoryUri;
    }

    @Override
    public List<Symbol> outputs() {
        return OUTPUTS;
    }

    public void inputs(List<Symbol> symbols) {
        inputs = symbols;
    }

    public List<Symbol> inputs() {
        return inputs;
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
        isDirectoryUri = in.readBoolean();
        uri = Symbol.fromStream(in);
        int size = in.readVInt();
        if (size > 0) {
            outputNames = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                outputNames.add(in.readString());
            }
        }
        int numInputs = in.readVInt();
        inputs = new ArrayList<>(numInputs);
        for (int i = 0; i < numInputs; i++) {
            inputs.add(Symbol.fromStream(in));
        }
        settings = ImmutableSettings.readSettingsFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isDirectoryUri);
        Symbol.toStream(uri, out);
        if (outputNames != null) {
            out.writeVInt(outputNames.size());
            for (String name : outputNames) {
                out.writeString(name);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeVInt(inputs.size());
        for (Symbol symbol : inputs) {
            Symbol.toStream(symbol, out);
        }
        ImmutableSettings.writeSettingsToStream(settings, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WriterProjection that = (WriterProjection) o;

        if (isDirectoryUri != that.isDirectoryUri) return false;
        if (outputNames != null ? !outputNames.equals(that.outputNames) : that.outputNames != null)
            return false;
        if (!settings.equals(that.settings)) return false;
        if (!uri.equals(that.uri)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + uri.hashCode();
        result = 31 * result + (isDirectoryUri ? 1 : 0);
        result = 31 * result + settings.hashCode();
        result = 31 * result + (outputNames != null ? outputNames.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WriterProjection{" +
                "uri=" + uri +
                ", settings=" + settings +
                ", outputNames=" + outputNames +
                ", isDirectory=" + isDirectoryUri +
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

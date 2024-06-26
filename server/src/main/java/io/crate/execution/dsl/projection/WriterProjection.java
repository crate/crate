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

package io.crate.execution.dsl.projection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataTypes;

public class WriterProjection extends Projection {

    // number of lines written
    private static final List<Symbol> OUTPUTS = List.of(new InputColumn(0, DataTypes.LONG));

    private final Symbol uri;
    private final List<Symbol> inputs;

    @Nullable
    private final List<String> outputNames;

    /*
     * add values that should be added or overwritten
     * all symbols must normalize to literals on the shard level.
     */
    private final Map<ColumnIdent, Symbol> overwrites;

    private final Settings withClauseOptions;

    private final OutputFormat outputFormat;

    public enum OutputFormat {
        JSON_OBJECT,
        JSON_ARRAY
    }

    private final CompressionType compressionType;

    public enum CompressionType {
        GZIP
    }

    public WriterProjection(List<Symbol> inputs,
                            Symbol uri,
                            @Nullable CompressionType compressionType,
                            Map<ColumnIdent, Symbol> overwrites,
                            @Nullable List<String> outputNames,
                            OutputFormat outputFormat,
                            Settings withClauseOptions) {
        this.inputs = inputs;
        this.uri = uri;
        this.overwrites = overwrites;
        this.outputNames = outputNames;
        this.outputFormat = outputFormat;
        this.compressionType = compressionType;
        this.withClauseOptions = withClauseOptions;
    }

    public WriterProjection(StreamInput in) throws IOException {
        uri = Symbol.fromStream(in);
        int size = in.readVInt();
        if (size > 0) {
            outputNames = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                outputNames.add(in.readString());
            }
        } else {
            outputNames = null;
        }
        inputs = Symbols.fromStream(in);
        int numOverwrites = in.readVInt();
        overwrites = new HashMap<>(numOverwrites);
        for (int i = 0; i < numOverwrites; i++) {
            overwrites.put(ColumnIdent.of(in), Symbol.fromStream(in));
        }
        int compressionTypeOrdinal = in.readInt();
        compressionType = compressionTypeOrdinal >= 0 ? CompressionType.values()[compressionTypeOrdinal] : null;
        outputFormat = OutputFormat.values()[in.readInt()];
        if (in.getVersion().onOrAfter(Version.V_4_8_0)) {
            withClauseOptions = Settings.readSettingsFromStream(in);
        } else {
            withClauseOptions = Settings.EMPTY;
        }
    }

    @Override
    public RowGranularity requiredGranularity() {
        return RowGranularity.SHARD;
    }

    public Symbol uri() {
        return uri;
    }

    @Override
    public List<Symbol> outputs() {
        return OUTPUTS;
    }

    public List<Symbol> inputs() {
        return inputs;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.WRITER;
    }

    public Map<ColumnIdent, Symbol> overwrites() {
        return this.overwrites;
    }

    public List<String> outputNames() {
        return this.outputNames;
    }

    public OutputFormat outputFormat() {
        return outputFormat;
    }

    public CompressionType compressionType() {
        return compressionType;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitWriterProjection(this, context);
    }

    public Settings withClauseOptions() {
        return withClauseOptions;
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
        Symbols.toStream(inputs, out);

        out.writeVInt(overwrites.size());
        for (Map.Entry<ColumnIdent, Symbol> entry : overwrites.entrySet()) {
            entry.getKey().writeTo(out);
            Symbol.toStream(entry.getValue(), out);
        }
        out.writeInt(compressionType != null ? compressionType.ordinal() : -1);
        out.writeInt(outputFormat.ordinal());
        if (out.getVersion().onOrAfter(Version.V_4_8_0)) {
            Settings.writeSettingsToStream(out, withClauseOptions);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WriterProjection that = (WriterProjection) o;

        if (!Objects.equals(outputNames, that.outputNames))
            return false;
        if (!uri.equals(that.uri)) return false;
        if (!overwrites.equals(that.overwrites)) return false;
        if (!Objects.equals(compressionType, that.compressionType))
            return false;
        if (!outputFormat.equals(that.outputFormat)) return false;
        return Objects.equals(withClauseOptions, that.withClauseOptions);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + uri.hashCode();
        result = 31 * result + (outputNames != null ? outputNames.hashCode() : 0);
        result = 31 * result + overwrites.hashCode();
        result = 31 * result + (compressionType != null ? compressionType.hashCode() : 0);
        result = 31 * result + outputFormat.hashCode();
        result = 31 * result + withClauseOptions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "WriterProjection{" +
               "uri=" + uri +
               ", outputNames=" + outputNames +
               ", compressionType=" + compressionType +
               ", outputFormat=" + outputFormat +
               ", withClauseOptions{" + withClauseOptions.toString() +
               '}';
    }

    public WriterProjection normalize(EvaluatingNormalizer normalizer, TransactionContext txnCtx) {
        Symbol nUri = normalizer.normalize(uri, txnCtx);
        if (uri != nUri) {
            return new WriterProjection(
                inputs,
                uri,
                compressionType,
                overwrites,
                outputNames,
                outputFormat,
                withClauseOptions
            );
        }
        return this;
    }
}

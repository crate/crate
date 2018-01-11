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

package io.crate.execution.dsl.projection;

import com.google.common.collect.ImmutableList;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.Value;
import io.crate.collections.Lists2;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.expression.scalar.FormatFunction;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import io.crate.types.StringType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriterProjection extends Projection {

    private static final List<Symbol> OUTPUTS = ImmutableList.<Symbol>of(
        new Value(DataTypes.LONG) // number of lines written
    );

    private static final Reference SHARD_ID_REF = new Reference(SysShardsTableInfo.ReferenceIdents.ID, RowGranularity.SHARD, IntegerType.INSTANCE);
    private static final Reference TABLE_NAME_REF = new Reference(SysShardsTableInfo.ReferenceIdents.TABLE_NAME, RowGranularity.SHARD, StringType.INSTANCE);
    private static final Reference PARTITION_IDENT_REF = new Reference(SysShardsTableInfo.ReferenceIdents.PARTITION_IDENT, RowGranularity.SHARD, StringType.INSTANCE);


    public static final Symbol DIRECTORY_TO_FILENAME = new Function(new FunctionInfo(
        new FunctionIdent(FormatFunction.NAME, Arrays.<DataType>asList(StringType.INSTANCE,
            StringType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE)),
        StringType.INSTANCE),
        Arrays.<Symbol>asList(Literal.of("%s_%s_%s.json"), TABLE_NAME_REF, SHARD_ID_REF, PARTITION_IDENT_REF)
    );

    private Symbol uri;
    private List<Symbol> inputs;

    @Nullable
    private List<String> outputNames;

    /*
     * add values that should be added or overwritten
     * all symbols must normalize to literals on the shard level.
     */
    private Map<ColumnIdent, Symbol> overwrites;

    private OutputFormat outputFormat;

    public enum OutputFormat {
        JSON_OBJECT,
        JSON_ARRAY
    }

    private CompressionType compressionType;

    public enum CompressionType {
        GZIP
    }

    public WriterProjection(List<Symbol> inputs,
                            Symbol uri,
                            @Nullable CompressionType compressionType,
                            Map<ColumnIdent, Symbol> overwrites,
                            @Nullable List<String> outputNames,
                            OutputFormat outputFormat) {
        this.inputs = inputs;
        this.uri = uri;
        this.overwrites = overwrites;
        this.outputNames = outputNames;
        this.outputFormat = outputFormat;
        this.compressionType = compressionType;
    }

    public WriterProjection(StreamInput in) throws IOException {
        uri = Symbols.fromStream(in);
        int size = in.readVInt();
        if (size > 0) {
            outputNames = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                outputNames.add(in.readString());
            }
        }
        inputs = Symbols.listFromStream(in);
        int numOverwrites = in.readVInt();
        overwrites = new HashMap<>(numOverwrites);
        for (int i = 0; i < numOverwrites; i++) {
            overwrites.put(new ColumnIdent(in), Symbols.fromStream(in));
        }
        int compressionTypeOrdinal = in.readInt();
        compressionType = compressionTypeOrdinal >= 0 ? CompressionType.values()[compressionTypeOrdinal] : null;
        outputFormat = OutputFormat.values()[in.readInt()];
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
    public void replaceSymbols(java.util.function.Function<? super Symbol, ? extends Symbol> replaceFunction) {
        Lists2.replaceItems(inputs, replaceFunction);
        for (Map.Entry<ColumnIdent, Symbol> entry : overwrites.entrySet()) {
            entry.setValue(replaceFunction.apply(entry.getValue()));
        }
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


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbols.toStream(uri, out);
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
            Symbols.toStream(entry.getValue(), out);
        }
        out.writeInt(compressionType != null ? compressionType.ordinal() : -1);
        out.writeInt(outputFormat.ordinal());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WriterProjection that = (WriterProjection) o;

        if (outputNames != null ? !outputNames.equals(that.outputNames) : that.outputNames != null)
            return false;
        if (!uri.equals(that.uri)) return false;
        if (!overwrites.equals(that.overwrites)) return false;
        if (compressionType != null ? !compressionType.equals(that.compressionType) : that.compressionType != null)
            return false;
        if (!outputFormat.equals(that.outputFormat)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + uri.hashCode();
        result = 31 * result + (outputNames != null ? outputNames.hashCode() : 0);
        result = 31 * result + overwrites.hashCode();
        result = 31 * result + (compressionType != null ? compressionType.hashCode() : 0);
        result = 31 * result + outputFormat.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "WriterProjection{" +
               "uri=" + uri +
               ", outputNames=" + outputNames +
               ", compressionType=" + compressionType +
               ", outputFormat=" + outputFormat +
               '}';
    }

    public WriterProjection normalize(EvaluatingNormalizer normalizer, TransactionContext transactionContext) {
        Symbol nUri = normalizer.normalize(uri, transactionContext);
        if (uri != nUri) {
            return new WriterProjection(
                inputs,
                uri,
                compressionType,
                overwrites,
                outputNames,
                outputFormat
            );
        }
        return this;
    }
}

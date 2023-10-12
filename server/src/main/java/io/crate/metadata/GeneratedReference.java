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

package io.crate.metadata;

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.format.Style;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;

public class GeneratedReference implements Reference {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(GeneratedReference.class);

    private final Reference ref;
    private final String formattedGeneratedExpression;
    @Nullable
    private Symbol generatedExpression;
    private List<Reference> referencedReferences = List.of();

    public GeneratedReference(Reference ref,
                              String formattedGeneratedExpression,
                              @Nullable Symbol generatedExpression) {
        this.ref = ref;
        this.formattedGeneratedExpression = formattedGeneratedExpression;
        generatedExpression(generatedExpression);
    }

    public GeneratedReference(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_5_0_0)) {
            ref = Reference.fromStream(in);
        } else {
            ref = new SimpleReference(in);
        }
        formattedGeneratedExpression = in.readString();
        if (in.getVersion().onOrAfter(Version.V_5_1_0)) {
            generatedExpression = Symbols.nullableFromStream(in);
        } else {
            generatedExpression = Symbols.fromStream(in);
        }
        int size = in.readVInt();
        referencedReferences = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            referencedReferences.add(Reference.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_5_0_0)) {
            Reference.toStream(out, ref);
        } else {
            if (ref instanceof SimpleReference simpleRef) {
                simpleRef.writeTo(out);
            } else {
                SimpleReference simpleReference = new SimpleReference(
                    ref.ident(),
                    ref.granularity(),
                    ref.valueType(),
                    ref.columnPolicy(),
                    ref.indexType(),
                    ref.isNullable(),
                    ref.hasDocValues(),
                    ref.position(),
                    ref.oid(),
                    ref.isDropped(),
                    ref.defaultExpression()
                );
                simpleReference.writeTo(out);
            }
        }
        out.writeString(formattedGeneratedExpression);
        if (out.getVersion().onOrAfter(Version.V_5_1_0)) {
            Symbols.nullableToStream(generatedExpression, out);
        } else {
            Symbols.toStream(generatedExpression, out);
        }

        out.writeVInt(referencedReferences.size());
        for (Reference reference : referencedReferences) {
            Reference.toStream(out, reference);
        }
    }

    public Reference reference() {
        return this.ref;
    }

    public String formattedGeneratedExpression() {
        return formattedGeneratedExpression;
    }

    public void generatedExpression(Symbol generatedExpression) {
        assert generatedExpression == null || generatedExpression.valueType().equals(valueType())
            : "The type of the generated expression must match the valueType of the `GeneratedReference`";
        this.generatedExpression = generatedExpression;
        if (generatedExpression != null && SymbolVisitors.any(Symbols::isAggregate, generatedExpression)) {
            throw new UnsupportedOperationException("Aggregation functions are not allowed in generated columns: " + generatedExpression);
        }
    }

    public Symbol generatedExpression() {
        assert generatedExpression != null : "Generated expression symbol must not be NULL, initialize first";
        return generatedExpression;
    }

    public void referencedReferences(List<Reference> references) {
        this.referencedReferences = references;
    }

    public List<Reference> referencedReferences() {
        return referencedReferences;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.GENERATED_REFERENCE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GeneratedReference that = (GeneratedReference) o;
        return Objects.equals(formattedGeneratedExpression, that.formattedGeneratedExpression) &&
               Objects.equals(generatedExpression, that.generatedExpression) &&
               Objects.equals(referencedReferences, that.referencedReferences) &&
               Objects.equals(ref, that.ref);
    }

    @Override
    public int hashCode() {
        return Objects.hash(generatedExpression, ref, referencedReferences);
    }

    @Override
    public String toString() {
        return toString(Style.UNQUALIFIED);
    }

    @Override
    public String toString(Style style) {
        return column().quotedOutputName() + " AS " + formattedGeneratedExpression;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitReference(this, context);
    }

    @Override
    public ReferenceIdent ident() {
        return ref.ident();
    }

    @Override
    public ColumnIdent column() {
        return ref.column();
    }

    @Override
    public IndexType indexType() {
        return ref.indexType();
    }

    @Override
    public ColumnPolicy columnPolicy() {
        return ref.columnPolicy();
    }

    @Override
    public boolean isNullable() {
        return ref.isNullable();
    }

    @Override
    public RowGranularity granularity() {
        return ref.granularity();
    }

    @Override
    public int position() {
        return ref.position();
    }

    @Override
    public long oid() {
        return ref.oid();
    }

    @Override
    public boolean isDropped() {
        return false;
    }

    @Override
    public boolean hasDocValues() {
        return ref.hasDocValues();
    }

    @Override
    public DataType<?> valueType() {
        return ref.valueType();
    }

    @Override
    public Symbol cast(DataType<?> targetType, CastMode... modes) {
        Symbol result = Reference.super.cast(targetType, modes);
        if (result == this) {
            return this;
        }
        if (result instanceof Reference castRef && !(result instanceof GeneratedReference)) {
            return new GeneratedReference(
                castRef,
                formattedGeneratedExpression,
                generatedExpression
            );
        }
        return result;
    }

    @Override
    public Symbol defaultExpression() {
        return ref.defaultExpression();
    }

    @Override
    public boolean isGenerated() {
        return true;
    }

    @Override
    public Reference withReferenceIdent(ReferenceIdent referenceIdent) {
        return new GeneratedReference(
            ref.withReferenceIdent(referenceIdent),
            formattedGeneratedExpression,
            generatedExpression
        );
    }

    @Override
    public Reference withColumnOid(LongSupplier oidSupplier) {
        if (ref.oid() != COLUMN_OID_UNASSIGNED) {
            return this;
        }
        return new GeneratedReference(
                ref.withColumnOid(oidSupplier),
                formattedGeneratedExpression,
                generatedExpression
        );
    }

    @Override
    public Reference withDropped(boolean dropped) {
        return new GeneratedReference(
            ref.withDropped(dropped),
            formattedGeneratedExpression,
            generatedExpression
        );
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE
            + ref.ramBytesUsed()
            + RamUsageEstimator.sizeOf(formattedGeneratedExpression)
            + (generatedExpression == null ? 0 : generatedExpression.ramBytesUsed())
            + referencedReferences.stream().mapToLong(Reference::ramBytesUsed).sum();
    }

    @Override
    public Map<String, Object> toMapping(int position) {
        return ref.toMapping(position);
    }
}

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

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.format.Style;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GeneratedReference extends Reference {

    private final String formattedGeneratedExpression;

    private Symbol generatedExpression;
    private List<Reference> referencedReferences;

    public GeneratedReference(StreamInput in) throws IOException {
        super(in);
        formattedGeneratedExpression = in.readString();
        generatedExpression = Symbols.fromStream(in);
        int size = in.readVInt();
        referencedReferences = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            referencedReferences.add(Reference.fromStream(in));
        }
    }

    public GeneratedReference(int position,
                              ReferenceIdent ident,
                              RowGranularity granularity,
                              DataType<?> type,
                              ColumnPolicy columnPolicy,
                              IndexType indexType,
                              String formattedGeneratedExpression,
                              boolean nullable,
                              boolean hasDocValues) {
        super(ident, granularity, type, columnPolicy, indexType, nullable, hasDocValues, position, null);
        this.formattedGeneratedExpression = formattedGeneratedExpression;
    }

    public GeneratedReference(int position,
                              ReferenceIdent ident,
                              RowGranularity granularity,
                              DataType<?> type,
                              String formattedGeneratedExpression) {
        super(ident, granularity, type, position, null);
        this.formattedGeneratedExpression = formattedGeneratedExpression;
    }

    public String formattedGeneratedExpression() {
        return formattedGeneratedExpression;
    }

    public void generatedExpression(Symbol generatedExpression) {
        this.generatedExpression = generatedExpression;
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
        if (!super.equals(o)) {
            return false;
        }
        GeneratedReference that = (GeneratedReference) o;
        return Objects.equals(formattedGeneratedExpression, that.formattedGeneratedExpression) &&
               Objects.equals(generatedExpression, that.generatedExpression) &&
               Objects.equals(referencedReferences, that.referencedReferences);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), formattedGeneratedExpression, generatedExpression, referencedReferences);
    }

    @Override
    public String toString(Style style) {
        return column().quotedOutputName() + " AS " + formattedGeneratedExpression;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(formattedGeneratedExpression);
        Symbols.toStream(generatedExpression, out);
        out.writeVInt(referencedReferences.size());
        for (Reference reference : referencedReferences) {
            Reference.toStream(reference, out);
        }
    }
}

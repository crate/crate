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

package io.crate.expression.reference.doc.lucene;

import java.io.IOException;
import java.util.function.UnaryOperator;

import org.apache.lucene.search.FieldDoc;
import org.jspecify.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.metadata.Reference;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;

/**
 * A {@link LuceneCollectorExpression} is used to collect
 * sorting values from FieldDocs
 */
public class OrderByCollectorExpression extends LuceneCollectorExpression<Object> {

    private final int orderIndex;
    private final UnaryOperator<Object> valueConversion;
    private final Object missingValue;
    // Some References use sentinel values in the FieldDocs to represent nulls, and in some cases
    // the sentinel value is also itself a valid value that can be stored in that column type (e.g.
    // REAL and DOUBLE). In these cases, we need to look up and return the actual value stored
    // in the document. When a Reference requires this, docValueSource is not null.
    //
    // When there is no ambiguity whether the Sentinel value represents NULL (e.g.
    // BigInt - the sentinel value is out of range for the type), it is unnecessary
    // to look up the value in the document because it can be safely inferred
    // from the FieldDocs and therefore docValueSource is null.
    private final LuceneCollectorExpression<?> docValueSource;
    private boolean ambiguous;

    private Object value;

    @Nullable
    private LuceneCollectorExpression<?> setDocValueSource(Reference ref) {
        DataType<?> dataType = ref.valueType();
        switch (dataType.id()) {
            case DoubleType.ID:
                return new DoubleColumnReference(ref.storageIdent());
            case FloatType.ID:
                return new FloatColumnReference(ref.storageIdent());
            default:
                return null;
        }
    }

    public OrderByCollectorExpression(Reference ref, OrderBy orderBy, UnaryOperator<Object> valueConversion) {
        this.docValueSource = setDocValueSource(ref);
        this.valueConversion = valueConversion;
        assert orderBy.orderBySymbols().contains(ref) : "symbol must be part of orderBy symbols";
        orderIndex = orderBy.orderBySymbols().indexOf(ref);
        this.missingValue = NullSentinelValues.nullSentinelForScoreDoc(orderBy, orderIndex);
    }

    private void value(Object value) {
        ambiguous = false;
        if (missingValue != null && missingValue.equals(value)) {
            // Cannot distinguish if the value stored in the document actually equals
            // the sentinel value (missingValue) or is null.
            // Mark this row as ambiguous to look up the value in the document later
            // after the caller of OrderByCollectorExpression advances the docValueSource
            // reader "pointer"
            ambiguous = true;
        } else {
            this.value = valueConversion.apply(value);
        }
    }

    @Override
    public void setNextReader(ReaderContext ctx) throws IOException {
        if (docValueSource != null) {
            docValueSource.setNextReader(ctx);
        }
    }

    @Override
    public void setNextDocId(int doc) {
        if (docValueSource != null) {
            docValueSource.setNextDocId(doc);
        }
    }

    public void setNextFieldDoc(FieldDoc fieldDoc) {
        value(fieldDoc.fields[orderIndex]);
    }

    @Override
    public Object value() {
        if (ambiguous) {
            if (docValueSource != null) {
                Object actualValue = docValueSource.value();
                return actualValue == null ? null : valueConversion.apply(actualValue);
            }
            return null;
        }
        return value;
    }

    @Override
    public String toString() {
        return "OrderByCollectorExpression{" +
               "idx=" + orderIndex +
               '}';
    }
}

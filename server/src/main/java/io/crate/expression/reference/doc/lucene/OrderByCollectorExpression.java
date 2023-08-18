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

import java.util.function.UnaryOperator;

import org.apache.lucene.search.FieldDoc;

import io.crate.analyze.OrderBy;
import io.crate.metadata.Reference;

/**
 * A {@link LuceneCollectorExpression} is used to collect
 * sorting values from FieldDocs
 */
public class OrderByCollectorExpression extends LuceneCollectorExpression<Object> {

    private final int orderIndex;
    private final UnaryOperator<Object> valueConversion;
    private final Object missingValue;

    private Object value;

    public OrderByCollectorExpression(Reference ref, OrderBy orderBy, UnaryOperator<Object> valueConversion) {
        this.valueConversion = valueConversion;
        assert orderBy.orderBySymbols().contains(ref) : "symbol must be part of orderBy symbols";
        orderIndex = orderBy.orderBySymbols().indexOf(ref);
        this.missingValue = NullSentinelValues.nullSentinelForScoreDoc(orderBy, orderIndex);
    }

    private void value(Object value) {
        if (missingValue != null && missingValue.equals(value)) {
            this.value = null;
        } else {
            this.value = valueConversion.apply(value);
        }
    }

    public void setNextFieldDoc(FieldDoc fieldDoc) {
        value(fieldDoc.fields[orderIndex]);
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public String toString() {
        return "OrderByCollectorExpression{" +
               "idx=" + orderIndex +
               '}';
    }
}

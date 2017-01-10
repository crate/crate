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

package io.crate.operation.reference.doc.lucene;

import io.crate.operation.Input;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorer;

/**
 * An expression which gets evaluated in the collect phase
 */
public abstract class LuceneCollectorExpression<ReturnType> implements Input<ReturnType> {

    final String columnName;

    public LuceneCollectorExpression(String columnName) {
        this.columnName = columnName;
    }

    public void startCollect(CollectorContext context) {

    }

    public void setNextDocId(int doc) {
    }

    public void setNextReader(LeafReaderContext context) {
    }

    public void setScorer(Scorer scorer) {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LuceneCollectorExpression<?> that = (LuceneCollectorExpression<?>) o;

        return columnName.equals(that.columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}

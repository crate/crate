/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Objects;

import org.apache.lucene.search.Scorable;

import io.crate.execution.engine.fetch.ReaderContext;

public class VirtualIdCollectorExpression extends LuceneCollectorExpression<String> {

    private final LuceneCollectorExpression<?> delegate;

    public VirtualIdCollectorExpression(LuceneCollectorExpression<?> delegate) {
        this.delegate = delegate;
    }

    @Override
    public String value() {
        return Objects.toString(delegate.value());
    }

    @Override
    public void startCollect(CollectorContext context) {
        delegate.startCollect(context);
    }

    @Override
    public void setNextDocId(int doc) {
        delegate.setNextDocId(doc);
    }

    @Override
    public void setNextReader(ReaderContext context) throws IOException {
        delegate.setNextReader(context);
    }

    @Override
    public void setScorer(Scorable scorer) {
        delegate.setScorer(scorer);
    }
}

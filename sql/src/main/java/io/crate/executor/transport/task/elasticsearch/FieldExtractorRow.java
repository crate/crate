/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.task.elasticsearch;

import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;

import java.util.List;

public class FieldExtractorRow<T> implements Row {

    private final List<FieldExtractor<T>> fieldExtractors;
    private T current;

    public FieldExtractorRow(List<FieldExtractor<T>> extractor) {
        fieldExtractors = extractor;
    }

    @Override
    public int size() {
        return fieldExtractors.size();
    }

    @Override
    public Object get(int index) {
        return fieldExtractors.get(index).extract(current);
    }

    @Override
    public Object[] materialize() {
        return Buckets.materialize(this);
    }

    public void setCurrent(T current) {
        this.current = current;
    }
}

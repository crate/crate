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

import com.google.common.base.Function;
import io.crate.metadata.Reference;

/**
 * Interface is used together with the {@link io.crate.executor.transport.task.elasticsearch.SymbolToFieldExtractor}
 * based on a defined action response.
 *
 * @param <T> The action response the converter is operating on
 * @param <C> A {@link io.crate.executor.transport.task.elasticsearch.SymbolToFieldExtractor.Context} implementation
 */
public interface FieldExtractorFactory<T, C extends SymbolToFieldExtractor.Context> {

    Function<T, Object> build(Reference reference, C context);

}

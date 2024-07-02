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

package io.crate.lucene;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;

import io.crate.common.collections.Lists;

/**
 * {@link CollectorManager} implementation that delegates to another
 * {@link CollectorManager} but wraps all Collectors
 */
public final class WrappingCollectorManager<CIn extends Collector, COut extends Collector, T>
        implements CollectorManager<COut, T> {

    private final CollectorManager<CIn, T> delegate;
    private final Function<? super CIn, ? extends COut> wrap;
    private final Function<? super COut, ? extends CIn> unwrap;

    public WrappingCollectorManager(CollectorManager<CIn, T> delegate,
                                    Function<? super CIn, ? extends COut> wrap,
                                    Function<? super COut, ? extends CIn> unwrap) {
        this.delegate = delegate;
        this.wrap = wrap;
        this.unwrap = unwrap;
    }

    @Override
    public COut newCollector() throws IOException {
        CIn collector = delegate.newCollector();
        return wrap.apply(collector);
    }

    @Override
    public T reduce(Collection<COut> collectors) throws IOException {
        return delegate.reduce(Lists.map(collectors, unwrap));
    }
}

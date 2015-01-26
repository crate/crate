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

package io.crate.executor;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * A pageable task result that consists of one single page only.
 * Intended use: when this page is the only one returned by a relation
 *
 * Ergo further paging is not supported and
 * {@linkplain #fetch(PageInfo)} returns an empty page.
 */
public class SinglePageTaskResult implements PageableTaskResult {

    private final Page page;

    public SinglePageTaskResult(Object[][] rows) {
        this(rows, 0, rows.length);
    }

    public SinglePageTaskResult(Object[][] rows, int start, int size) {
        this.page = new ObjectArrayPage(rows, start, size);
    }

    @Override
    public ListenableFuture<PageableTaskResult> fetch(PageInfo pageInfo) {
        return Futures.immediateFuture(PageableTaskResult.EMPTY_PAGEABLE_RESULT);
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public Object[][] rows() {
        throw new UnsupportedOperationException("rows() not supported on SinglePageTaskResult");
    }

    @Nullable
    @Override
    public String errorMessage() {
        return null;
    }

    @Override
    public void close() throws IOException {
        // boomshakalakka!
    }
}

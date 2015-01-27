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

package io.crate.executor.pageable;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.TaskResult;
import io.crate.executor.pageable.policy.PageCachePolicy;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * enhanced TaskResult that supports fetching pages
 * asynchronously.
 *
 */
public interface PageableTaskResult extends TaskResult, Closeable {

    public static final PageableTaskResult EMPTY_PAGABLE_RESULT = new PageableTaskResult() {
        @Override
        public ListenableFuture<Page> fetch(PageInfo pageInfo) {
            return Futures.immediateFailedFuture(new NoSuchElementException());
        }

        @Override
        public Page page() {
            return Page.EMPTY;
        }

        @Override
        public PageCachePolicy policy() {
            return PageCachePolicy.NO_CACHE;
        }

        @Override
        public Object[][] rows() {
            throw new UnsupportedOperationException("rows() is not supported on PageableTaskResut");
        }

        @Nullable
        @Override
        public String errorMessage() {
            return null;
        }

        @Override
        public void close() throws IOException {
            // shalalalalalala!
        }
    };

    /**
     * get the page identified by <code>pageInfo</code>.
     *
     * @param pageInfo identifying the page to fetch
     * @return a future holding the page identified by pageInfo
     */
    ListenableFuture<Page> fetch(PageInfo pageInfo);


    /**
     *
     * <strong>
     *     Use this instead of {@linkplain #rows()}!
     * </strong>
     * <p>
     *
     * Get the page that was fetched with this task result.
     * It might contain less rows than requested.
     * @return a Page you can iterate over to get the rows it consists of
     */
    Page page();

    /**
     * The policy determines how this taskResult handles stuff already fetched.
     * How much rows/pages are kept locally before accessing its sources
     */
    PageCachePolicy policy();

    /**
     * Must be called after paging is done
     * as the resources necessary for paging must be cleared
     */
    @Override
    void close() throws IOException;
}

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

package io.crate.executor.pageable.policy;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.pageable.Page;
import io.crate.executor.pageable.PageInfo;

import java.util.TreeMap;

public class CachingPageCache implements PageCache {

    private final PageCachePolicy.CachingPolicy policy;
    private final TreeMap<PageInfo, Page> pageCache;
    private final Function<PageInfo, ListenableFuture<Page>> fetchPageCallable;
    private long numRows;

    public CachingPageCache(PageCachePolicy.CachingPolicy policy, final Function<PageInfo, ListenableFuture<Page>> fetchPageCallable) {
        this.policy = policy;
        this.fetchPageCallable = fetchPageCallable;
        this.pageCache = new TreeMap<>(Ordering.natural());

        this.numRows = 0;
    }

    public ListenableFuture<Page> get(final PageInfo pageInfo) {
        Page page = pageCache.get(pageInfo);
        if (page == null) {
            if ((policy.hasMaxPages() && pageCache.size() + 1 >= policy.maxNumPages()) ||
                    (policy.hasMaxRows() && numRows + pageInfo.size() >= policy.maxNumRows())) {
                long numRowsRemoved = 0;
                if (policy.retentionPolicy() == PageCachePolicy.CachingPolicy.RetentionPolicy.REMOVE_HEAD) {
                    numRowsRemoved = pageCache.remove(pageCache.firstKey()).size();
                } else {
                    numRowsRemoved = pageCache.remove(pageCache.lastKey()).size();
                }
                numRows -= numRowsRemoved;
            }

            try {
                final SettableFuture<Page> pageFuture = SettableFuture.create();
                ListenableFuture<Page> fetchPageFuture= fetchPageCallable.apply(pageInfo);
                assert fetchPageFuture != null;
                Futures.addCallback(fetchPageFuture, new FutureCallback<Page>() {
                    @Override
                    public void onSuccess(Page result) {
                        pageCache.put(pageInfo, result);
                        numRows += result.size();
                        pageFuture.set(result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        pageFuture.setException(t);
                    }
                });
                return pageFuture;

            } catch (Throwable e) {
                throw Throwables.propagate(e);
            }
        } else {
            return Futures.immediateFuture(page);
        }
    }
}

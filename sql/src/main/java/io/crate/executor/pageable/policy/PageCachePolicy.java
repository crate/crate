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

public interface PageCachePolicy {

    public static final PageCachePolicy NO_CACHE = new PageCachePolicy() {};
    public static final PageCachePolicy.CachingPolicy CACHE_ALL = new CachingPolicy(-1, -1, CachingPolicy.RetentionPolicy.REMOVE_HEAD);

    static class CachingPolicy implements PageCachePolicy {


        public static enum RetentionPolicy {
            REMOVE_HEAD,
            REMOVE_TAIL
        }

        private final RetentionPolicy retentionPolicy;
        private final int maxNumPages;
        private final int maxNumRows;

        public CachingPolicy(RetentionPolicy retentionPolicy) {
            this(-1, -1, retentionPolicy);
        }

        public static CachingPolicy pageBased(int maxNumPages, RetentionPolicy retentionPolicy) {
            return new CachingPolicy(maxNumPages, -1, retentionPolicy);
        }

        public static CachingPolicy rowBased(int maxNumRows, RetentionPolicy retentionPolicy) {
            return new CachingPolicy(-1, maxNumRows, retentionPolicy);
        }

        public CachingPolicy(int maxNumPages, int maxNumRows, RetentionPolicy retentionPolicy) {
            this.retentionPolicy = retentionPolicy;
            this.maxNumPages = maxNumPages;
            this.maxNumRows = maxNumRows;
        }

        public RetentionPolicy retentionPolicy() {
            return retentionPolicy;
        }

        public boolean hasMaxPages() {
            return maxNumPages != -1;
        }

        public boolean hasMaxRows() {
            return maxNumRows != -1;
        }

        public int maxNumPages() {
            return maxNumPages;
        }

        public int maxNumRows() {
            return maxNumRows;
        }
    }

}

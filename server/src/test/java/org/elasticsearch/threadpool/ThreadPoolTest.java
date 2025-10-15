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

package org.elasticsearch.threadpool;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class ThreadPoolTest extends ESTestCase {

    @Test
    public void test_one_eight_of_processors() {
        assertThat(ThreadPool.oneEightOfProcessors(1)).isEqualTo(1);
        assertThat(ThreadPool.oneEightOfProcessors(3)).isEqualTo(1);
        assertThat(ThreadPool.oneEightOfProcessors(8)).isEqualTo(1);
        assertThat(ThreadPool.oneEightOfProcessors(16)).isEqualTo(2);
    }


    @Test
    public void test_force_merge_pool_size() {
        final int processors = randomIntBetween(1, EsExecutors.numberOfProcessors(Settings.EMPTY));
        final ThreadPool threadPool = new TestThreadPool(
            "test",
            Settings.builder().put(EsExecutors.PROCESSORS_SETTING.getKey(), processors).build()
        );
        try {
            final int expectedSize = Math.max(1, processors / 8);
            ThreadPool.Info info = threadPool.info(ThreadPool.Names.FORCE_MERGE);
            assertThat(info).isNotNull();
            assertThat(info.min()).isEqualTo(expectedSize);
            assertThat(info.max()).isEqualTo(expectedSize);
        } finally {
            terminate(threadPool);
        }

    }
}

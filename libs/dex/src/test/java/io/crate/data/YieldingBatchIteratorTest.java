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

package io.crate.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

public class YieldingBatchIteratorTest {

    @Test
    public void test() throws Exception {
        try (ExecutorService executorService = Executors.newSingleThreadExecutor()) {
            var source = InMemoryBatchIterator.of(List.of(1, 1, 1, 2, 3), null, false);
            BatchIterator<Integer> bi = YieldingBatchIterator.of(source, executorService, Duration.ofNanos(1));
            assertThat(bi.moveNext()).isTrue();
            assertThat(bi.moveNext()).isFalse();
            assertThat(bi.allLoaded()).isFalse();
            CompletionStage<?> nextBatch = bi.loadNextBatch();
            assertThat(nextBatch).succeedsWithin(Duration.ofSeconds(1));
            assertThat(bi.moveNext()).isTrue();
            assertThat(bi.allLoaded()).isTrue();
        }
    }
}


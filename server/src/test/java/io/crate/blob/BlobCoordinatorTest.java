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

package io.crate.blob;

import static io.crate.testing.Asserts.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class BlobCoordinatorTest extends ESTestCase {

    @Test
    public void testSameDigestGetsSameSemaphore() throws Exception {
        BlobCoordinator blobCoordinator = new BlobCoordinator();
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        try {
            Set<Semaphore> semaphoresForSameDigest = new HashSet<>();
            List<CompletableFuture<Semaphore>> futures = new ArrayList<>(100);
            for (int i = 0; i < 100; i++) {
                futures.add(CompletableFuture.supplyAsync(
                    () -> blobCoordinator.digestCoordinator("testDigest"),
                    executorService));
            }

            futures.forEach(future -> semaphoresForSameDigest.add(future.join()));

            assertThat(semaphoresForSameDigest).hasSize(1);
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}

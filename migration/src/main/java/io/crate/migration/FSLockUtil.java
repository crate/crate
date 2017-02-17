/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.migration;

import org.apache.lucene.store.*;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

class FSLockUtil implements Closeable {

    private final Lock[] locks;
    private final Environment environment;

    FSLockUtil(Environment environment) {
        this.environment = environment;
        locks = new Lock[environment.dataWithClusterFiles().length];
    }

    void obtainLocks() throws IllegalArgumentException {
        int maxLocalStorageNodes = environment.settings().getAsInt("node.max_local_storage_nodes", 50);
        for (int possibleLockId = 0; possibleLockId < maxLocalStorageNodes; possibleLockId++) {
            for (int dirIndex = 0; dirIndex < environment.dataWithClusterFiles().length; dirIndex++) {
                Path dir = environment.dataWithClusterFiles()[dirIndex].resolve(NodeEnvironment.NODES_FOLDER)
                    .resolve(Integer.toString(possibleLockId));

                try (Directory luceneDir = FSDirectory.open(dir, NativeFSLockFactory.INSTANCE)) {
                    try {
                        locks[dirIndex] = luceneDir.obtainLock(NodeEnvironment.NODE_LOCK_FILENAME);
                    } catch (LockObtainFailedException e) {
                        releaseLocks();
                        throw new IllegalStateException("failed to obtain lock on " + dir.toAbsolutePath(), e);
                    }
                } catch (IOException e) {
                    releaseLocks();
                    throw new IllegalStateException("failed to obtain lock on " + dir.toAbsolutePath(), e);
                }
            }
        }

        if (locks[0] == null) {
            releaseLocks();
            throw new IllegalStateException("Failed to obtain node lock, is the following location writable?: "
                                            + Arrays.toString(environment.dataWithClusterFiles()));
        }
    }

    private void releaseLocks() {
        for (int i = 0; i < locks.length; i++) {
            if (locks[i] != null) {
                IOUtils.closeWhileHandlingException(locks[i]);
            }
            locks[i] = null;
        }
    }

    @Override
    public void close() throws IOException {
        releaseLocks();
    }
}

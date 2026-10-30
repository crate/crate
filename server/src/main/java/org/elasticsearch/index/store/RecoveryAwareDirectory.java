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

package org.elasticsearch.index.store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.elasticsearch.index.shard.ShardId;

/**
 * Lets a {@link org.apache.lucene.store.Directory} seed its local translog before peer recovery's
 * own pre-recovery check runs - needed only where local Lucene data can exist without a local
 * translog, unlike every ordinary {@code Directory}. Checked via
 * {@link org.apache.lucene.store.FilterDirectory#unwrap}, since {@code Store} always wraps it.
 */
public interface RecoveryAwareDirectory {

    /** No-op for any {@code Directory} that doesn't need it. */
    void maybeSeedLocalTranslog(Map<String, String> commitUserData,
                                Path translogLocation,
                                ShardId shardId,
                                long primaryTerm) throws IOException;
}

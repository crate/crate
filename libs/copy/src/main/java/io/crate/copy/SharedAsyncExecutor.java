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

package io.crate.copy;

import java.io.IOException;

import org.apache.opendal.AsyncExecutor;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;

/**
 * Wrapper around OpenDAL AsyncExecutor.
 * Holds a singleton instance and manages its lifecycle.
 */
public class SharedAsyncExecutor extends AbstractLifecycleComponent {

    private final AsyncExecutor asyncExecutor;

    public SharedAsyncExecutor(Settings settings) {
        int numOfProcessors = EsExecutors.numberOfProcessors(settings);
        this.asyncExecutor = AsyncExecutor.createTokioExecutor(numOfProcessors);
    }

    public AsyncExecutor asyncExecutor() {
        return asyncExecutor;
    }

    @Override
    protected void doStart() {
        // No-op.
    }

    @Override
    protected void doStop() {
        // No-op.
    }

    @Override
    protected void doClose() throws IOException {
        asyncExecutor.close();
    }
}

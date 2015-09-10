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

package io.crate.jobs;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.concurrent.CancellationException;

public abstract class AbstractExecutionSubContext implements ExecutionSubContext, ExecutionState {

    protected static final ESLogger LOGGER = Loggers.getLogger(AbstractExecutionSubContext.class);

    protected final SubExecutionContextFuture future = new SubExecutionContextFuture();
    private final int id;

    protected KeepAliveListener keepAliveListener;
    private volatile boolean isKilled = false;

    protected AbstractExecutionSubContext(int id) {
        this.id = id;
    }

    @Override
    public void keepAliveListener(KeepAliveListener listener) {
        keepAliveListener = listener;
    }

    public int id() {
        return id;
    }

    protected void innerPrepare() {
    }

    @Override
    final public void prepare() {
        if (!future.closed()) {
            LOGGER.trace("preparing {}: {}", this, id);
            try {
                innerPrepare();
            } catch (Throwable t) {
                close(t);
            }
        }
    }

    protected void innerStart() {
    }

    @Override
    final public void start() {
        if (!future.closed()) {
            LOGGER.trace("starting {}: {}", this, id);
            try {
                innerStart();
            } catch (Throwable t) {
                close(t);
            }
        }
    }

    protected void innerClose(@Nullable Throwable t) {
    }

    protected boolean close(@Nullable Throwable t) {
        if (future.firstClose()) {
            LOGGER.trace("closing {}: {}", this, id);
            try {
                innerClose(t);
            } catch (Throwable t2) {
                if (t == null) {
                    t = t2;
                } else {
                    LOGGER.warn("closing due to exception, but closing also throws exception", t2);
                }
            } finally {
                cleanup();
            }
            future.close(t);
            return true;
        }
        return false;
    }

    @Override
    final public void close() {
        close(null);
    }

    protected void innerKill(@Nullable Throwable t) {
    }

    @Override
    final public void kill(@Nullable Throwable t) {
        if (future.firstClose()) {
            if (t == null) {
                t = new CancellationException();
            }
            LOGGER.trace("killing {}: {}", this, id);
            try {
                innerKill(t);
            } catch (Throwable t2) {
                LOGGER.warn("killing due to exception, but killing also throws exception", t2);
            } finally {
                cleanup();
            }
            isKilled = true;
            future.close(t);
        }
    }

    @Override
    final public SubExecutionContextFuture future() {
        return future;
    }

    @Override
    public boolean isKilled() {
        return isKilled;
    }


    /**
     * Hook to cleanup resources of this context. This is called in finally clauses on kill and close.
     * This hook might be called more than one time, therefore it should be idempotent.
     */
    protected void cleanup() {

    }

}

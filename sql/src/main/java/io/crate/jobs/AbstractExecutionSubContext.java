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

import io.crate.concurrent.CompletionListener;
import io.crate.exceptions.JobKilledException;
import org.elasticsearch.common.logging.ESLogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class AbstractExecutionSubContext implements ExecutionSubContext {

    protected final ESLogger logger;
    protected final SubExecutionContextFuture future = new SubExecutionContextFuture();
    protected final int id;

    protected AbstractExecutionSubContext(int id, ESLogger logger) {
        this.id = id;
        this.logger = logger;
    }

    public int id() {
        return id;
    }

    protected void innerPrepare() {
    }

    @Override
    final public void prepare() {
        if (!future.closed()) {
            logger.trace("preparing id={} ctx={}", id, this);
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
            logger.trace("starting id={} ctx={}", id, this);
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
            logger.trace("closing id={} ctx={}", id, this);
            try {
                innerClose(t);
            } catch (Throwable t2) {
                if (t == null) {
                    t = t2;
                } else {
                    logger.warn("closing due to exception, but closing also throws exception", t2);
                }
            } finally {
                cleanup();
            }
            future.close(t);
            return true;
        }
        return false;
    }

    final public void close() {
        close(null);
    }

    protected void innerKill(@Nonnull Throwable t) {
    }

    @Override
    final public void kill(@Nullable Throwable t) {
        if (future.firstClose()) {
            if (t == null) {
                t = new InterruptedException(JobKilledException.MESSAGE);
            }
            logger.trace("killing id={} ctx={} cause={}", id, this, t);
            try {
                innerKill(t);
            } catch (Throwable t2) {
                logger.warn("killing due to exception, but killing also throws exception", t2);
            } finally {
                cleanup();
            }
            future.close(t);
        }
    }

    /**
     * Hook to cleanup resources of this context. This is called in finally clauses on kill and close.
     * This hook might be called more than one time, therefore it should be idempotent.
     */
    protected void cleanup() {

    }

    @Override
    public void addListener(final CompletionListener listener) {
        future.addCallback(listener);
    }
}

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

package io.crate.breaker;

import io.crate.planner.node.ExecutionPhase;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.logging.Loggers;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

public class RamAccountingContext {

    // Flush every 2mb
    public static long FLUSH_BUFFER_SIZE = 1024 * 1024 * 2;

    private final String contextId;
    private final CircuitBreaker breaker;

    private final AtomicLong totalBytes = new AtomicLong(0);
    private final AtomicLong flushBuffer = new AtomicLong(0);
    private volatile boolean closed = false;
    private volatile boolean tripped = false;

    private static final Logger logger = Loggers.getLogger(RamAccountingContext.class);

    public static RamAccountingContext forExecutionPhase(CircuitBreaker breaker, ExecutionPhase executionPhase) {
        String ramAccountingContextId = String.format(Locale.ENGLISH, "%s: %d",
            executionPhase.name(), executionPhase.phaseId());
        return new RamAccountingContext(ramAccountingContextId, breaker);
    }

    public RamAccountingContext(String contextId, CircuitBreaker breaker) {
        this.contextId = contextId;
        this.breaker = breaker;
    }

    /**
     * Add bytes to the context and maybe break
     *
     * @param bytes bytes to be added
     * @throws CircuitBreakingException in case the breaker tripped
     */
    public void addBytes(long bytes) throws CircuitBreakingException {
        addBytes(bytes, true);
    }

    /**
     * Add bytes to the context without breaking
     *
     * @param bytes bytes to be added
     */
    public void addBytesWithoutBreaking(long bytes) {
        addBytes(bytes, false);
    }

    private void addBytes(long bytes, boolean shouldBreak) throws CircuitBreakingException {
        if (closed || bytes == 0) {
            return;
        }
        long currentFlushBuffer = flushBuffer.addAndGet(bytes);
        if (currentFlushBuffer >= FLUSH_BUFFER_SIZE) {
            if (shouldBreak) {
                flush(currentFlushBuffer);
            } else {
                flushWithoutBreaking(currentFlushBuffer);
            }
        }
    }

    /**
     * Flush the {@code bytes} to the breaker, incrementing the total
     * bytes and adjusting the buffer.
     *
     * @param bytes long value of bytes to be flushed to the breaker
     * @throws CircuitBreakingException in case the breaker tripped
     */
    private void flush(long bytes) throws CircuitBreakingException {
        if (bytes == 0) {
            return;
        }
        try {
            breaker.addEstimateBytesAndMaybeBreak(bytes, contextId);
        } catch (CircuitBreakingException e) {
            // since we've already created the data, we need to
            // add it so closing the context re-adjusts properly
            breaker.addWithoutBreaking(bytes);
            tripped = true;
            // re-throw the original exception
            throw e;
        } finally {
            totalBytes.addAndGet(bytes);
            flushBuffer.addAndGet(-bytes);
        }
    }

    /**
     * Flush the {@code bytes} to the breaker, incrementing the total
     * bytes and adjusting the buffer.
     *
     * @param bytes long value of bytes to be flushed to the breaker
     */
    private void flushWithoutBreaking(long bytes) {
        if (bytes == 0) {
            return;
        }
        breaker.addWithoutBreaking(bytes);
        if (exceededBreaker()) {
            tripped = true;
        }
        totalBytes.addAndGet(bytes);
        flushBuffer.addAndGet(-bytes);
    }

    /**
     * Returns bytes from the buffer + bytes that have already been flushed to the breaker.
     * @return the total number of bytes that have been aggregated
     */
    public long totalBytes() {
        return flushBuffer.get() + totalBytes.get();
    }

    /**
     * Close the context and adjust the breaker.
     * A remaining flush buffer will not be flushed to avoid breaking on close.
     * (all ram operations expected to be finished at this point)
     */
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (totalBytes.get() != 0) {
            if (logger.isTraceEnabled() && totalBytes() > FLUSH_BUFFER_SIZE) {
                logger.trace("context: {} bytes; breaker: {} of {} bytes", totalBytes(), breaker.getUsed(), breaker.getLimit());
            }
            breaker.addWithoutBreaking(-totalBytes.get());
        }
        totalBytes.addAndGet(flushBuffer.getAndSet(0));
    }

    /**
     * Returns true if the limit of the breaker was already reached
     */
    public boolean trippedBreaker() {
        return tripped;
    }

    /**
     * Returns true if the limit of the breaker was already reached
     * but the breaker did not trip (e.g. when adding bytes without breaking)
     */
    public boolean exceededBreaker() {
        return breaker.getUsed() >= breaker.getLimit();
    }

    /**
     * Returns the configured bytes limit of the breaker
     */
    public long limit() {
        return breaker.getLimit();
    }

    /**
     * Returns the context id string.
     */
    public String contextId() {
        return contextId;
    }


    /**
     * round n up to the nearest multiple of m
     */
    public static long roundUp(long n, long m) {
        return n + (n % m);
    }

    /**
     * round n up to the nearest multiple of 8
     */
    public static long roundUp(long n) {
        return roundUp(n, 8);
    }
}

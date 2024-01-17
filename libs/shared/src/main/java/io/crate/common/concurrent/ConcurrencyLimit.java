/**
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.crate.common.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



/**
 * This class is based on the `Gradient2Limit` from netflix/concurrency-limits
 *
 *
 * Concurrency limit algorithm that adjusts the limit based on the gradient of change of the current average RTT and
 * a long term exponentially smoothed average RTT.  Unlike traditional congestion control algorithms we use average
 * instead of minimum since RPC methods can be very bursty due to various factors such as non-homogenous request
 * processing complexity as well as a wide distribution of data size.  We have also found that using minimum can result
 * in an bias towards an impractically low base RTT resulting in excessive load shedding.  An exponential decay is
 * applied to the base RTT so that the value is kept stable yet is allowed to adapt to long term changes in latency
 * characteristics.
 *
 * The core algorithm re-calculates the limit every sampling window (ex. 1 second) using the formula
 *
 *      // Calculate the gradient limiting to the range [0.5, 1.0] to filter outliers
 *      gradient = max(0.5, min(1.0, longtermRtt / currentRtt));
 *
 *      // Calculate the new limit by applying the gradient and allowing for some queuing
 *      newLimit = gradient * currentLimit + queueSize;
 *
 *      // Update the limit using a smoothing factor (default 0.2)
 *      newLimit = currentLimit * (1-smoothing) + newLimit * smoothing
 *
 * The limit can be in one of three main states
 *
 * 1.  Steady state
 *
 * In this state the average RTT is very stable and the current measurement whipsaws around this value, sometimes reducing
 * the limit, sometimes increasing it.
 *
 * 2.  Transition from steady state to load
 *
 * In this state either the RPS to latency has spiked. The gradient is {@literal <} 1.0 due to a growing request queue that
 * cannot be handled by the system. Excessive requests and rejected due to the low limit. The baseline RTT grows using
 * exponential decay but lags the current measurement, which keeps the gradient {@literal <} 1.0 and limit low.
 *
 * 3.  Transition from load to steady state
 *
 * In this state the system goes back to steady state after a prolonged period of excessive load.  Requests aren't rejected
 * and the sample RTT remains low. During this state the long term RTT may take some time to go back to normal and could
 * potentially be several multiples higher than the current RTT.
 */
public final class ConcurrencyLimit {

    private static final Logger LOG = LogManager.getLogger(ConcurrencyLimit.class);

    /**
     * Estimated concurrency limit based on our algorithm
     */
    private volatile double estimatedLimit;

    /**
     * Tracks a measurement of the short time, and more volatile, RTT meant to represent the current system latency
     */
    private long lastRtt;

    /**
     * Tracks a measurement of the long term, less volatile, RTT meant to represent the baseline latency.  When the system
     * is under load this number is expect to trend higher.
     */
    private final Measurement longRtt;

    /**
     * Maximum allowed limit providing an upper bound failsafe
     */
    private final int maxLimit;

    private final int minLimit;

    private final IntFunction<Integer> queueSize;

    private final double smoothing;

    private final double tolerance;

    private volatile int limit;

    private final AtomicInteger numInflight = new AtomicInteger();

    public ConcurrencyLimit(int initialLimit,
                            int minConcurrency,
                            int maxConcurrency,
                            IntFunction<Integer> queueSize,
                            double smoothing,
                            int longWindow,
                            double rttTolerance) {
        this.limit = initialLimit;
        this.estimatedLimit = initialLimit;
        this.minLimit = minConcurrency;
        this.maxLimit = maxConcurrency;
        this.queueSize = queueSize;
        this.smoothing = smoothing;
        this.tolerance = rttTolerance;
        this.lastRtt = 0;
        this.longRtt = new ExpAvgMeasurement(longWindow, 10);
    }

    /**
     * Start a sample, increasing the number of inflight operations and returning a
     * monotonic time value which should be used for `onSample`
     **/
    public long startSample() {
        numInflight.incrementAndGet();
        return System.nanoTime();
    }

    public final void onSample(long startTime, boolean didDrop) {
        long rtt = System.nanoTime() - startTime;
        int decrementedNumInflight = numInflight.decrementAndGet();
        synchronized (this) {
            int newLimit = update(rtt, decrementedNumInflight, didDrop);
            if (newLimit != limit) {
                limit = newLimit;
            }
        }
    }

    public final int getLimit() {
        return limit;
    }

    private int update(final long rtt, final int inflight, final boolean didDrop) {
        final double queueSize = this.queueSize.apply((int)this.estimatedLimit);

        this.lastRtt = rtt;
        final double shortRtt = (double)rtt;
        final double longRtt = this.longRtt.add(rtt);


        // If the long RTT is substantially larger than the short RTT then reduce the long RTT measurement.
        // This can happen when latency returns to normal after a prolonged prior of excessive load.  Reducing the
        // long RTT without waiting for the exponential smoothing helps bring the system back to steady state.
        if (longRtt / shortRtt > 2) {
            this.longRtt.update(current -> current * 0.95);
        }

        // Don't grow the limit if we are app limited
        if (inflight < estimatedLimit / 2) {
            return (int) estimatedLimit;
        }

        // Rtt could be higher than rtt_noload because of smoothing rtt noload updates
        // so set to 1.0 to indicate no queuing.  Otherwise calculate the slope and don't
        // allow it to be reduced by more than half to avoid aggressive load-shedding due to
        // outliers.
        final double gradient = Math.max(0.5, Math.min(1.0, tolerance * longRtt / shortRtt));
        double newLimit = estimatedLimit * gradient + queueSize;
        newLimit = estimatedLimit * (1 - smoothing) + newLimit * smoothing;
        newLimit = Math.max(minLimit, Math.min(maxLimit, newLimit));

        if ((int)estimatedLimit != newLimit) {
            LOG.debug("New limit={} lastRtt={} ms longRtt={} ms queueSize={} gradient={} inflight={}",
                    (int)newLimit,
                    getLastRtt(TimeUnit.MICROSECONDS) / 1000.0,
                    getLongRtt(TimeUnit.MICROSECONDS) / 1000.0,
                    queueSize,
                    gradient,
                    inflight);
        }

        estimatedLimit = newLimit;

        return (int)estimatedLimit;
    }

    public long getLastRtt(TimeUnit units) {
        return units.convert(lastRtt, TimeUnit.NANOSECONDS);
    }

    public long getLongRtt(TimeUnit units) {
        return units.convert((long) longRtt.get(), TimeUnit.NANOSECONDS);
    }

    @Override
    public String toString() {
        return "GradientLimit [limit=" + (int)estimatedLimit + "]";
    }

    public boolean exceedsLimit() {
        return numInflight() > limit;
    }

    public int numInflight() {
        return numInflight.get();
    }
}

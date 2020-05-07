/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.rounding;

import io.crate.common.unit.TimeValue;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.IllegalInstantException;

import java.util.Objects;

/**
 * A strategy for rounding long values.
 */
public abstract class Rounding {

    public abstract byte id();

    /**
     * Rounds the given value.
     */
    public abstract long round(long value);


    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

    public static Builder builder(DateTimeUnit unit) {
        return new Builder(unit);
    }

    public static Builder builder(TimeValue interval) {
        return new Builder(interval);
    }

    public static class Builder {

        private final DateTimeUnit unit;
        private final long interval;

        private DateTimeZone timeZone = DateTimeZone.UTC;

        public Builder(DateTimeUnit unit) {
            this.unit = unit;
            this.interval = -1;
        }

        public Builder(TimeValue interval) {
            this.unit = null;
            if (interval.millis() < 1)
                throw new IllegalArgumentException("Zero or negative time interval not supported");
            this.interval = interval.millis();
        }

        public Builder timeZone(DateTimeZone timeZone) {
            if (timeZone == null) {
                throw new IllegalArgumentException("Setting null as timezone is not supported");
            }
            this.timeZone = timeZone;
            return this;
        }

        public Rounding build() {
            Rounding timeZoneRounding;
            if (unit != null) {
                timeZoneRounding = new TimeUnitRounding(unit, timeZone);
            } else {
                timeZoneRounding = new TimeIntervalRounding(interval, timeZone);
            }
            return timeZoneRounding;
        }
    }

    static class TimeUnitRounding extends Rounding {

        static final byte ID = 1;

        private final DateTimeUnit unit;
        private final DateTimeField field;
        private final DateTimeZone timeZone;
        private final boolean unitRoundsToMidnight;

        TimeUnitRounding(DateTimeUnit unit, DateTimeZone timeZone) {
            this.unit = unit;
            this.field = unit.field(timeZone);
            unitRoundsToMidnight = this.field.getDurationField().getUnitMillis() > 60L * 60L * 1000L;
            this.timeZone = timeZone;
        }

        @Override
        public byte id() {
            return ID;
        }

        /**
         * @return The latest timestamp T which is strictly before utcMillis
         * and such that timeZone.getOffset(T) != timeZone.getOffset(utcMillis).
         * If there is no such T, returns Long.MAX_VALUE.
         */
        private long previousTransition(long utcMillis) {
            final int offsetAtInputTime = timeZone.getOffset(utcMillis);
            do {
                // Some timezones have transitions that do not change the offset, so we have to
                // repeatedly call previousTransition until a nontrivial transition is found.

                long previousTransition = timeZone.previousTransition(utcMillis);
                if (previousTransition == utcMillis) {
                    // There are no earlier transitions
                    return Long.MAX_VALUE;
                }
                assert previousTransition < utcMillis; // Progress was made
                utcMillis = previousTransition;
            } while (timeZone.getOffset(utcMillis) == offsetAtInputTime);

            return utcMillis;
        }

        @Override
        public long round(long utcMillis) {

            // field.roundFloor() works as long as the offset doesn't change.  It is worth getting this case out of the way first, as
            // the calculations for fixing things near to offset changes are a little expensive and are unnecessary in the common case
            // of working in UTC.
            if (timeZone.isFixed()) {
                return field.roundFloor(utcMillis);
            }

            // When rounding to hours we consider any local time of the form 'xx:00:00' as rounded, even though this gives duplicate
            // bucket names for the times when the clocks go back. Shorter units behave similarly. However, longer units round down to
            // midnight, and on the days where there are two midnights we would rather pick the earlier one, so that buckets are
            // uniquely identified by the date.
            if (unitRoundsToMidnight) {
                final long anyLocalStartOfDay = field.roundFloor(utcMillis);
                // `anyLocalStartOfDay` is _supposed_ to be the Unix timestamp for the start of the day in question in the current time
                // zone.  Mostly this just means "midnight", which is fine, and on days with no local midnight it's the first time that
                // does occur on that day which is also ok. However, on days with >1 local midnight this is _one_ of the midnights, but
                // may not be the first. Check whether this is happening, and fix it if so.

                final long previousTransition = previousTransition(anyLocalStartOfDay);

                if (previousTransition == Long.MAX_VALUE) {
                    // No previous transitions, so there can't be another earlier local midnight.
                    return anyLocalStartOfDay;
                }

                final long currentOffset = timeZone.getOffset(anyLocalStartOfDay);
                final long previousOffset = timeZone.getOffset(previousTransition);
                assert currentOffset != previousOffset;

                // NB we only assume interference from one previous transition. It's theoretically possible to have two transitions in
                // quick succession, both of which have a midnight in them, but this doesn't appear to happen in the TZDB so (a) it's
                // pointless to implement and (b) it won't be tested. I recognise that this comment is tempting fate and will likely
                // cause this very situation to occur in the near future, and eagerly look forward to fixing this using a loop over
                // previous transitions when it happens.

                final long alsoLocalStartOfDay = anyLocalStartOfDay + currentOffset - previousOffset;
                // `alsoLocalStartOfDay` is the Unix timestamp for the start of the day in question if the previous offset were in
                // effect.

                if (alsoLocalStartOfDay <= previousTransition) {
                    // Therefore the previous offset _is_ in effect at `alsoLocalStartOfDay`, and it's earlier than anyLocalStartOfDay,
                    // so this is the answer to use.
                    return alsoLocalStartOfDay;
                } else {
                    // The previous offset is not in effect at `alsoLocalStartOfDay`, so the current offset must be.
                    return anyLocalStartOfDay;
                }

            } else {
                do {
                    long rounded = field.roundFloor(utcMillis);

                    // field.roundFloor() mostly works as long as the offset hasn't changed in [rounded, utcMillis], so look at where
                    // the offset most recently changed.

                    final long previousTransition = previousTransition(utcMillis);

                    if (previousTransition == Long.MAX_VALUE || previousTransition < rounded) {
                        // The offset did not change in [rounded, utcMillis], so roundFloor() worked as expected.
                        return rounded;
                    }

                    // The offset _did_ change in [rounded, utcMillis]. Put differently, this means that none of the times in
                    // [previousTransition+1, utcMillis] were rounded, so the rounded time must be <= previousTransition.  This means
                    // it's sufficient to try and round previousTransition down.
                    assert previousTransition < utcMillis;
                    utcMillis = previousTransition;
                } while (true);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(unit, timeZone);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TimeUnitRounding other = (TimeUnitRounding) obj;
            return Objects.equals(unit, other.unit) && Objects.equals(timeZone, other.timeZone);
        }

        @Override
        public String toString() {
            return "[" + timeZone + "][" + unit + "]";
        }
    }

    static class TimeIntervalRounding extends Rounding {

        static final byte ID = 2;

        private final long interval;
        private final DateTimeZone timeZone;

        TimeIntervalRounding(long interval, DateTimeZone timeZone) {
            if (interval < 1)
                throw new IllegalArgumentException("Zero or negative time interval not supported");
            this.interval = interval;
            this.timeZone = timeZone;
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long round(long utcMillis) {
            long timeLocal = timeZone.convertUTCToLocal(utcMillis);
            long rounded = roundKey(timeLocal, interval) * interval;
            long roundedUTC;
            if (isInDSTGap(rounded) == false) {
                roundedUTC = timeZone.convertLocalToUTC(rounded, true, utcMillis);
                // check if we crossed DST transition, in this case we want the
                // last rounded value before the transition
                long transition = timeZone.previousTransition(utcMillis);
                if (transition != utcMillis && transition > roundedUTC) {
                    roundedUTC = round(transition - 1);
                }
            } else {
                /*
                 * Edge case where the rounded local time is illegal and landed
                 * in a DST gap. In this case, we choose 1ms tick after the
                 * transition date. We don't want the transition date itself
                 * because those dates, when rounded themselves, fall into the
                 * previous interval. This would violate the invariant that the
                 * rounding operation should be idempotent.
                 */
                roundedUTC = timeZone.previousTransition(utcMillis) + 1;
            }
            return roundedUTC;
        }

        private static long roundKey(long value, long interval) {
            if (value < 0) {
                return (value - interval + 1) / interval;
            } else {
                return value / interval;
            }
        }

        /**
         * Determine whether the local instant is a valid instant in the given
         * time zone. The logic for this is taken from
         * {@link DateTimeZone#convertLocalToUTC(long, boolean)} for the
         * `strict` mode case, but instead of throwing an
         * {@link IllegalInstantException}, which is costly, we want to return a
         * flag indicating that the value is illegal in that time zone.
         */
        private boolean isInDSTGap(long instantLocal) {
            if (timeZone.isFixed()) {
                return false;
            }
            // get the offset at instantLocal (first estimate)
            int offsetLocal = timeZone.getOffset(instantLocal);
            // adjust instantLocal using the estimate and recalc the offset
            int offset = timeZone.getOffset(instantLocal - offsetLocal);
            // if the offsets differ, we must be near a DST boundary
            if (offsetLocal != offset) {
                // determine if we are in the DST gap
                long nextLocal = timeZone.nextTransition(instantLocal - offsetLocal);
                if (nextLocal == (instantLocal - offsetLocal)) {
                    nextLocal = Long.MAX_VALUE;
                }
                long nextAdjusted = timeZone.nextTransition(instantLocal - offset);
                if (nextAdjusted == (instantLocal - offset)) {
                    nextAdjusted = Long.MAX_VALUE;
                }
                if (nextLocal != nextAdjusted) {
                    // we are in the DST gap
                    return true;
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(interval, timeZone);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TimeIntervalRounding other = (TimeIntervalRounding) obj;
            return Objects.equals(interval, other.interval) && Objects.equals(timeZone, other.timeZone);
        }
    }
}

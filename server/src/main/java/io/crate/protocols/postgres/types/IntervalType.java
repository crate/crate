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

package io.crate.protocols.postgres.types;

import java.nio.charset.StandardCharsets;

import org.joda.time.Period;
import org.joda.time.ReadablePeriod;

import io.crate.types.Regproc;
import io.netty.buffer.ByteBuf;

public class IntervalType extends PGType<Period> {

    private static final int OID = 1186;
    private static final int TYPE_LEN = 16;
    private static final int TYPE_MOD = -1;
    public static final IntervalType INSTANCE = new IntervalType();

    private IntervalType() {
        super(OID, TYPE_LEN, TYPE_MOD, "interval");
    }

    @Override
    public int typArray() {
        return PGArray.INTERVAL_ARRAY.oid();
    }

    @Override
    public String typeCategory() {
        return TypeCategory.TIMESPAN.code();
    }

    @Override
    public String type() {
        return Type.BASE.code();
    }

    @Override
    public Regproc typSend() {
        return Regproc.of("interval_send");
    }

    @Override
    public Regproc typReceive() {
        return Regproc.of("interval_recv");
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, Period period) {
        buffer.writeInt(TYPE_LEN);
        // from PostgreSQL code:
        // pq_sendint64(&buf, interval->time);
        // pq_sendint32(&buf, interval->day);
        // pq_sendint32(&buf, interval->month);
        buffer.writeLong(
            (period.getHours() * 60 * 60 * 1000_000L)
            + (period.getMinutes() * 60 * 1000_000L)
            + (period.getSeconds() * 1000_000L)
            + (period.getMillis() * 1000)
        );
        buffer.writeInt((period.getWeeks() * 7) + period.getDays());
        buffer.writeInt((period.getYears() * 12) + period.getMonths());
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public Period readBinaryValue(ByteBuf buffer, int valueLength) {
        assert valueLength == TYPE_LEN : "length should be " + TYPE_LEN + " because interval is 16. Actual length: " +
                                         valueLength;
        long micros = buffer.readLong();
        int days = buffer.readInt();
        int months = buffer.readInt();

        long microsInAnHour = 60 * 60 * 1000_000L;
        int hours = Math.toIntExact(micros / microsInAnHour);
        long microsWithoutHours = micros % microsInAnHour;

        long microsInAMinute = 60 * 1000_000L;
        int minutes = Math.toIntExact(microsWithoutHours / microsInAMinute);
        long microsWithoutMinutes = microsWithoutHours % microsInAMinute;

        int seconds = Math.toIntExact(microsWithoutMinutes / 1000_000);
        int millis = Math.toIntExact((microsWithoutMinutes % 1000_000) / 1000);
        return new Period(
            months / 12,
            months % 12,
            days / 7,
            days % 7,
            hours,
            minutes,
            seconds,
            millis
        );
    }

    @Override
    byte[] encodeAsUTF8Text(Period value) {
        StringBuffer sb = new StringBuffer();
        io.crate.types.IntervalType.PERIOD_FORMATTER.printTo(sb, (ReadablePeriod) value);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    Period decodeUTF8Text(byte[] bytes) {
        return io.crate.types.IntervalType.PERIOD_FORMATTER.parsePeriod(new String(bytes, StandardCharsets.UTF_8));
    }
}

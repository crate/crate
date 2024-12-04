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

package io.crate.expression.scalar;


import java.util.EnumSet;

import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.PeriodType;

import io.crate.data.Input;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.types.DataTypes;

public class AgeFunction extends Scalar<Period, Object> {

    private static final FunctionName NAME = new FunctionName(PgCatalogSchemaInfo.NAME, "age");

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.TIMESTAMP.getTypeSignature())
                .returnType(DataTypes.INTERVAL.getTypeSignature())
                .features(EnumSet.of(Feature.STRICTNULL))
                .build(),
            AgeFunction::new
        );

        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.TIMESTAMP.getTypeSignature(),
                    DataTypes.TIMESTAMP.getTypeSignature())
                .returnType(DataTypes.INTERVAL.getTypeSignature())
                .features(EnumSet.of(Feature.STRICTNULL))
                .build(),
            AgeFunction::new
        );
    }


    public AgeFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Period evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
        /*
         We cannot create Period from millis difference
         since sometimes we need to treat different millis range as same interval.
         Postgres treats age between first days of the sequential months as
         '1 month' despite on possibly different 28, 30, 31 days/millis range.
         */
        assert args.length > 0 && args.length < 3 : "Invalid number of arguments";

        var arg1 = args[0].value();
        if (arg1 == null) {
            return null;
        }
        if (args.length == 1) {
            long curDateMillis = txnCtx.currentInstant().toEpochMilli();
            curDateMillis = curDateMillis - curDateMillis % 86400000; // current_date at midnight, similar to CurrentDateFunction implementation.
            return getPeriod(curDateMillis, (long) arg1);
        } else {
            var arg2 = args[1].value();
            if (arg2 == null) {
                return null;
            }
            return getPeriod((long) arg1, (long) arg2);
        }
    }

    /**
     * returns Period in yearMonthDayTime which corresponds to Postgres default Interval output format.
     * See https://www.postgresql.org/docs/14/datatype-datetime.html#DATATYPE-INTERVAL-OUTPUT
     */
    public static Period getPeriod(long timestamp1, long timestamp2) {
        /*
         PeriodType is important as it affects the internal representation of the Period object.
         PeriodType.yearMonthDayTime() is needed to return 8 days but not 1 week 1 day.
         Streamer of the IntervalType will simply put 0 in 'out.writeVInt(p.getWeeks())' as getWeeks() returns zero for unused fields.
         */

        if (timestamp1 < timestamp2) {
            /*
            In Postgres second argument is subtracted from the first.
            Interval's first argument must be smaller than second and thus we swap params and negate.

            We need to pass UTC timezone to be sure that Interval doesn't end up using system default time zone.
            Currently, timestamps are in UTC (see https://github.com/crate/crate/issues/10037 and
            https://github.com/crate/crate/issues/12064) but if https://github.com/crate/crate/issues/7196 ever gets
            implemented, we need to pass here not UTC but time zone set by SET TIMEZONE.
            */
            return new Interval(timestamp1, timestamp2, DateTimeZone.UTC).toPeriod(PeriodType.yearMonthDayTime()).negated();
        } else {
            return new Interval(timestamp2, timestamp1, DateTimeZone.UTC).toPeriod(PeriodType.yearMonthDayTime());
        }
    }
}

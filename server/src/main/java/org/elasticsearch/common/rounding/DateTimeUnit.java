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

import java.util.function.Function;

import org.elasticsearch.common.joda.Joda;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

public enum DateTimeUnit {

    WEEK_OF_WEEKYEAR(tz -> ISOChronology.getInstance(tz).weekOfWeekyear()),
    YEAR_OF_CENTURY(tz -> ISOChronology.getInstance(tz).yearOfCentury()),
    QUARTER(tz -> Joda.QUARTER_OF_YEAR.getField(ISOChronology.getInstance(tz))),
    MONTH_OF_YEAR(tz -> ISOChronology.getInstance(tz).monthOfYear()),
    DAY_OF_MONTH(tz -> ISOChronology.getInstance(tz).dayOfMonth()),
    HOUR_OF_DAY(tz -> ISOChronology.getInstance(tz).hourOfDay()),
    MINUTES_OF_HOUR(tz -> ISOChronology.getInstance(tz).minuteOfHour()),
    SECOND_OF_MINUTE(tz -> ISOChronology.getInstance(tz).secondOfMinute());

    private final Function<DateTimeZone, DateTimeField> fieldFunction;

    DateTimeUnit(Function<DateTimeZone, DateTimeField> fieldFunction) {
        this.fieldFunction = fieldFunction;
    }

    /**
     * @return the {@link DateTimeField} for the provided {@link DateTimeZone} for this time unit
     */
    public DateTimeField field(DateTimeZone tz) {
        return fieldFunction.apply(tz);
    }
}

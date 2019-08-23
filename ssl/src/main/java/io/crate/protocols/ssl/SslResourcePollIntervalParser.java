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

package io.crate.protocols.ssl;

import com.sun.nio.file.SensitivityWatchEventModifier;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.TimeValue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class SslResourcePollIntervalParser implements Function<String, TimeValue> {

    private static final List<Long> EFFECTIVE_VALUES = List.of(2L, 10L, 30L);

    private final Logger logger;

    public SslResourcePollIntervalParser(Logger logger) {
        this.logger = logger;
    }

    @Override
    public TimeValue apply(String s) {
        TimeValue value = TimeValue.parseTimeValue(s, SslConfigSettings.SSL_RESOURCE_POLL_INTERVAL_NAME);
        final long seconds = value.getSeconds();
        long effectiveSeconds = seconds;
        if (EFFECTIVE_VALUES.stream().anyMatch(l -> l == seconds) == false) {
            if (seconds < SensitivityWatchEventModifier.HIGH.sensitivityValueInSeconds()) {
                logger.warn("Value {} is smaller than the minimum effective value of {}s, will use {}s instead.",
                            value.getStringRep(),
                            SensitivityWatchEventModifier.HIGH.sensitivityValueInSeconds(),
                            SensitivityWatchEventModifier.HIGH.sensitivityValueInSeconds());
                effectiveSeconds = SensitivityWatchEventModifier.HIGH.sensitivityValueInSeconds();
            } else if (seconds < SensitivityWatchEventModifier.MEDIUM.sensitivityValueInSeconds()) {
                logger.warn("Value {} is smaller than the next effective value of {}s, will use {}s instead.",
                            value.getStringRep(),
                            SensitivityWatchEventModifier.MEDIUM.sensitivityValueInSeconds(),
                            SensitivityWatchEventModifier.MEDIUM.sensitivityValueInSeconds());
                effectiveSeconds = SensitivityWatchEventModifier.MEDIUM.sensitivityValueInSeconds();
            } else if (seconds < SensitivityWatchEventModifier.LOW.sensitivityValueInSeconds()) {
                logger.warn("Value {} is smaller than the next effective value of {}s, will use {}s instead.",
                            value.getStringRep(),
                            SensitivityWatchEventModifier.LOW.sensitivityValueInSeconds(),
                            SensitivityWatchEventModifier.LOW.sensitivityValueInSeconds());
                effectiveSeconds = SensitivityWatchEventModifier.LOW.sensitivityValueInSeconds();
            } else if (seconds > SensitivityWatchEventModifier.LOW.sensitivityValueInSeconds()) {
                logger.warn("Value {} is greater than the maximum effective value of {}s, will use {}s instead.",
                            value.getStringRep(),
                            SensitivityWatchEventModifier.LOW.sensitivityValueInSeconds(),
                            SensitivityWatchEventModifier.LOW.sensitivityValueInSeconds());
                effectiveSeconds = SensitivityWatchEventModifier.LOW.sensitivityValueInSeconds();
            }
        }
        return new TimeValue(effectiveSeconds, TimeUnit.SECONDS);
    }
}

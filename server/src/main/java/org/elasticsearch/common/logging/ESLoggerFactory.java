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

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.spi.ExtendedLogger;

/**
 * Factory to get {@link Logger}s
 */
final class ESLoggerFactory {

    private ESLoggerFactory() {

    }

    static Logger getLogger(String prefix, String name) {
        return getLogger(prefix, LogManager.getLogger(name));
    }

    static Logger getLogger(String prefix, Class<?> clazz) {
        /*
         * At one point we didn't use LogManager.getLogger(clazz) because
         * of a bug in log4j that has since been fixed:
         * https://github.com/apache/logging-log4j2/commit/ae33698a1846a5e10684ec3e52a99223f06047af
         *
         * For now we continue to use LogManager.getLogger(clazz.getName())
         * because we expect to eventually migrate away from needing this
         * method entirely.
         */
        return getLogger(prefix, LogManager.getLogger(clazz.getName()));
    }

    static Logger getLogger(String prefix, Logger logger) {
        /*
         * In a followup we'll throw an exception if prefix is null or empty
         * redirecting folks to LogManager.getLogger.
         *
         * This and more is tracked in https://github.com/elastic/elasticsearch/issues/32174
         */
        if (prefix == null || prefix.length() == 0) {
            return logger;
        }
        return new PrefixLogger((ExtendedLogger)logger, logger.getName(), prefix);
    }
}

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

package org.elasticsearch.rest;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.elasticsearch.common.Strings;

public class RestUtils {

    /**
     * Determine if CORS setting is a regex
     *
     * @return a corresponding {@link Pattern} if so and o.w. null.
     */
    public static Pattern checkCorsSettingForRegex(String corsSetting) {
        if (corsSetting == null) {
            return null;
        }
        int len = corsSetting.length();
        boolean isRegex = len > 2 && corsSetting.startsWith("/") && corsSetting.endsWith("/");

        if (isRegex) {
            return Pattern.compile(corsSetting.substring(1, corsSetting.length() - 1));
        }
        return null;
    }

    /**
     * Return the CORS setting as an array of origins.
     *
     * @param corsSetting the CORS allow origin setting as configured by the user;
     *                    should never pass null, but we check for it anyway.
     * @return an array of origins if set, otherwise {@code null}.
     */
    public static String[] corsSettingAsArray(String corsSetting) {
        if (Strings.isNullOrEmpty(corsSetting)) {
            return new String[0];
        }
        return Arrays.stream(corsSetting.split(","))
                     .map(String::trim)
                     .toArray(String[]::new);
    }
}

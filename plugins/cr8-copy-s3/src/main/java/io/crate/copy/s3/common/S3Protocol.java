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

package io.crate.copy.s3.common;

import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;

public enum S3Protocol {
    HTTPS,
    HTTP;

    private static final String NAME = "protocol";

    private static final Setting<S3Protocol> PROTOCOL = new Setting<>(
        NAME,
        HTTPS.toString(),
        p -> {
            for (S3Protocol s3Protocol : S3Protocol.values()) {
                if (s3Protocol.toString().equals(p)) {
                    return s3Protocol;
                }
            }
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "invalid protocol `%s`. Expected HTTP or HTTPS", p));
        },
        DataTypes.STRING,
        Setting.Property.Final
    );

    public static String get(Settings settings) {
        return PROTOCOL.get(settings).toString();
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ENGLISH);
    }
}

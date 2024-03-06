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

package io.crate.types;

import io.crate.Streamer;
import io.crate.statistics.ColumnStatsSupport;

public class NotSupportedType extends DataType<Void> {

    public static final NotSupportedType INSTANCE = new NotSupportedType();
    public static final int ID = 1;

    private NotSupportedType() {
    }

    @Override
    public boolean isConvertableTo(DataType<?> other, boolean explicitCast) {
        return false;
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.NOT_SUPPORTED;
    }

    @Override
    public String getName() {
        return "NOT SUPPORTED";
    }

    @Override
    public Streamer<Void> streamer() {
        return null;
    }

    @Override
    public Void implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return null;
    }

    @Override
    public Void sanitizeValue(Object value) {
        return null;
    }

    @Override
    public int compare(Void val1, Void val2) {
        return 0;
    }

    @Override
    public int compareTo(DataType<?> o) {
        return 0;
    }

    @Override
    public long valueBytes(Void value) {
        return 0;
    }
}

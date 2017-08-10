/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

public class NotSupportedType extends DataType<Void> {

    public final static NotSupportedType INSTANCE = new NotSupportedType();
    public final static int ID = 1;

    private NotSupportedType() {
    }

    @Override
    public boolean isConvertableTo(DataType other) {
        return false;
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "NOT SUPPORTED";
    }

    @Override
    public Streamer<?> streamer() {
        return null;
    }

    @Override
    public Void value(Object value) {
        return null;
    }

    @Override
    public int compareValueTo(Void val1, Void val2) {
        return 0;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}

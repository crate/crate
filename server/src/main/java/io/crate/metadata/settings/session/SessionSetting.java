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

package io.crate.metadata.settings.session;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.settings.SessionSettings;
import io.crate.types.DataType;

public class SessionSetting<T> {

    private final String name;
    private final Function<Object[], T> parse;
    private final BiConsumer<CoordinatorSessionSettings, T> setter;
    private final Function<SessionSettings, String> getter;
    private final Supplier<String> defaultValue;

    private final String description;
    private final DataType<?> type;

    public SessionSetting(String name,
                   Function<Object[], T> parse,
                   BiConsumer<CoordinatorSessionSettings, T> setter,
                   Function<SessionSettings, String> getter,
                   Supplier<String> defaultValue,
                   String description,
                   DataType<?> type) {
        this.name = name;
        this.parse = parse;
        this.setter = setter;
        this.getter = getter;
        this.defaultValue = defaultValue;
        this.description = description;
        this.type = type;
    }

    public void apply(CoordinatorSessionSettings sessionSettings,
                      List<Symbol> symbols,
                      Function<? super Symbol, Object> eval) {
        Object[] values = new Object[symbols.size()];
        for (int i = 0; i < symbols.size(); i++) {
            Symbol symbol = symbols.get(i);
            values[i] = eval.apply(symbol);
        }
        T converted = parse.apply(values);
        setter.accept(sessionSettings, converted);
    }

    public String getValue(SessionSettings sessionSettings) {
        return getter.apply(sessionSettings);
    }

    public String defaultValue() {
        return defaultValue.get();
    }

    public String description() {
        return description;
    }

    public DataType<?> type() {
        return type;
    }

    public String name() {
        return name;
    }
}

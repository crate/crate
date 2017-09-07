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

package io.crate.metadata.settings;

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.expressions.ExpressionToByteSizeValueVisitor;
import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.analyze.expressions.ExpressionToObjectVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.analyze.expressions.ExpressionToTimeValueVisitor;
import io.crate.data.Row;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.types.BooleanType;
import io.crate.types.IntegerType;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class SettingsAppliers {

    public abstract static class AbstractSettingsApplier implements SettingsApplier {
        protected final String name;
        final Settings defaultSettings;

        public AbstractSettingsApplier(String name, Settings defaultSettings) {
            this.name = name;
            this.defaultSettings = defaultSettings;
        }

        @Override
        public Settings getDefault() {
            return defaultSettings;
        }

        public void applyValue(Settings.Builder settingsBuilder, Object value) {
            try {
                settingsBuilder.put(name, validate(value));
            } catch (InvalidSettingValueContentException e) {
                throw new IllegalArgumentException(e.getMessage());
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
        }

        @VisibleForTesting
        public Object validate(Object value) {
            return value;
        }

        protected IllegalArgumentException invalidException(Exception cause) {
            return new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Invalid value for argument '%s'", name), cause);
        }

        protected IllegalArgumentException invalidException() {
            return new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Invalid value for argument '%s'", name));
        }

        @Override
        public String toString() {
            return "SettingsApplier{" + "name='" + name + '\'' + '}';
        }
    }

    public abstract static class NumberSettingsApplier extends AbstractSettingsApplier {

        protected final Setting setting;

        NumberSettingsApplier(Setting setting) {
            super(setting.name(), setting.defaultValue() == null ? Settings.EMPTY :
                Settings.builder().put(setting.name(), setting.defaultValue()).build());
            this.setting = setting;
        }

        @Override
        public void apply(Settings.Builder settingsBuilder, Row parameters, Expression expression) {
            try {
                applyValue(settingsBuilder, ExpressionToNumberVisitor.convert(expression, parameters));
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
        }

    }

    public static class BooleanSettingsApplier extends AbstractSettingsApplier {

        public BooleanSettingsApplier(BoolSetting setting) {
            super(setting.name(),
                Settings.builder().put(setting.name(), setting.defaultValue()).build());
        }

        @Override
        public void apply(Settings.Builder settingsBuilder, Row parameters, Expression expression) {
            try {
                applyValue(settingsBuilder, ExpressionToObjectVisitor.convert(expression, parameters));
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
        }

        @Override
        public Object validate(Object value) {
            return BooleanType.INSTANCE.value(value);
        }
    }

    public static class IntSettingsApplier extends NumberSettingsApplier {

        public IntSettingsApplier(IntSetting setting) {
            super(setting);
        }

        @Override
        @VisibleForTesting
        public Object validate(Object value) {
            Integer convertedValue = IntegerType.INSTANCE.value(value);
            IntSetting setting = (IntSetting) this.setting;
            if (convertedValue < setting.minValue() || convertedValue > setting.maxValue()) {
                throw invalidException();
            }
            return convertedValue;
        }
    }

    public static class ByteSizeSettingsApplier extends AbstractSettingsApplier {

        private final ByteSizeSetting setting;

        public ByteSizeSettingsApplier(ByteSizeSetting setting) {
            super(setting.name(), setting.defaultValue() == null ? Settings.EMPTY :
                Settings.builder().put(setting.name(), setting.defaultValue()).build());
            this.setting = setting;
        }

        private ByteSizeValue validate(ByteSizeValue num) {
            if (num.getBytes() < setting.minValue() || num.getBytes() > setting.maxValue()) {
                throw invalidException();
            }
            return num;
        }

        @Override
        public void apply(Settings.Builder settingsBuilder, Row parameters, Expression expression) {
            ByteSizeValue byteSizeValue;
            try {
                byteSizeValue = ExpressionToByteSizeValueVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }

            if (byteSizeValue == null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "'%s' does not support null values", name));
            }

            applyValue(settingsBuilder, byteSizeValue);
        }

        void applyValue(Settings.Builder settingsBuilder, ByteSizeValue value) {
            value = validate(value);
            settingsBuilder.put(name, value.getBytes(), ByteSizeUnit.BYTES);
        }

        @Override
        public void applyValue(Settings.Builder settingsBuilder, Object value) {
            ByteSizeValue byteSizeValue;
            if (value instanceof Number) {
                byteSizeValue = new ByteSizeValue(((Number) value).longValue());
            } else if (value instanceof String) {
                try {
                    byteSizeValue = ByteSizeValue.parseBytesSizeValue((String) value,
                        ExpressionToByteSizeValueVisitor.DEFAULT_VALUE.toString());
                } catch (ElasticsearchParseException e) {
                    throw invalidException(e);
                }
            } else {
                throw invalidException();
            }
            applyValue(settingsBuilder, byteSizeValue);
        }
    }

    public static class StringSettingsApplier extends AbstractSettingsApplier {

        private final StringSetting setting;

        public StringSettingsApplier(StringSetting setting) {
            super(setting.name(), setting.defaultValue() == null ? Settings.EMPTY :
                Settings.builder().put(setting.name(), setting.defaultValue()).build());
            this.setting = setting;
        }

        @Override
        @VisibleForTesting
        public Object validate(Object value) {
            String validation = this.setting.validate((String) value);
            if (validation != null) {
                throw new InvalidSettingValueContentException(validation);
            }
            return value;
        }

        @Override
        public void apply(Settings.Builder settingsBuilder, Row parameters, Expression expression) {
            if (expression instanceof ObjectLiteral) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Object values are not allowed at '%s'", name));
            }
            applyValue(settingsBuilder, ExpressionToStringVisitor.convert(expression, parameters));
        }
    }

    public static class TimeSettingsApplier extends SettingsAppliers.AbstractSettingsApplier {

        private final TimeSetting setting;

        public TimeSettingsApplier(TimeSetting setting) {
            super(setting.name(),
                Settings.builder().put(setting.name(), setting.defaultValue()).build());
            this.setting = setting;
        }

        private TimeValue validate(TimeValue value) {
            if (value.getMillis() < setting.minValue().getMillis() ||
                value.getMillis() > setting.maxValue().getMillis()) {
                throw invalidException();
            }
            return value;
        }

        @Override
        public void apply(Settings.Builder settingsBuilder, Row parameters, Expression expression) {
            TimeValue time;
            try {
                time = ExpressionToTimeValueVisitor.convert(expression, parameters, name);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            applyValue(settingsBuilder, time.millis());
        }

        void applyValue(Settings.Builder settingsBuilder, TimeValue value) {
            value = validate(value);
            settingsBuilder.put(name, value.getMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public void applyValue(Settings.Builder settingsBuilder, Object value) {
            TimeValue timeValue;
            if (value instanceof String) {
                try {
                    timeValue = TimeValue.parseTimeValue((String) value, ExpressionToTimeValueVisitor.DEFAULT_VALUE, name);
                } catch (ElasticsearchParseException e) {
                    throw invalidException(e);
                }

            } else if (value instanceof Number) {
                timeValue = new TimeValue(((Number) value).longValue());
            } else {
                throw invalidException();
            }
            applyValue(settingsBuilder, timeValue);
        }
    }

    static class InvalidSettingValueContentException extends IllegalArgumentException {
        InvalidSettingValueContentException(String message) {
            super(message);
        }
    }
}

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

package io.crate.analyze;

import io.crate.metadata.settings.*;
import io.crate.sql.tree.Expression;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Locale;

public class SettingsAppliers {

    public static abstract class AbstractSettingsApplier implements SettingsApplier {
        protected final String name;
        protected final Settings defaultSettings;

        public AbstractSettingsApplier(String name, Settings defaultSettings) {
            this.name = name;
            this.defaultSettings = defaultSettings;
        }

        @Override
        public Settings getDefault() {
            return defaultSettings;
        }

        public IllegalArgumentException invalidException(Exception cause) {
            return new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid value for argument '%s'", name), cause);
        }

        public IllegalArgumentException invalidException() {
            return new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid value for argument '%s'", name));
        }
    }

    public static class ObjectSettingsApplier extends AbstractSettingsApplier {

        public ObjectSettingsApplier(NestedSetting settings) {
            super(settings.settingName(),
                    ImmutableSettings.builder().put(settings.settingName(), settings.defaultValue()).build());
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            Object value = null;
            try {
                value = ExpressionToObjectVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            settingsBuilder.put(this.name, value);
        }
    }

    public static class BooleanSettingsApplier extends AbstractSettingsApplier {

        public BooleanSettingsApplier(BoolSetting setting) {
            super(setting.settingName(),
                    ImmutableSettings.builder().put(setting.settingName(), setting.defaultValue()).build());
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            Boolean value;
            try {
                value = DataTypes.BOOLEAN.value(ExpressionToObjectVisitor.convert(expression, parameters));
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            settingsBuilder.put(name, value);
        }
    }

    public static class IntSettingsApplier extends AbstractSettingsApplier {

        private final IntSetting setting;

        public IntSettingsApplier(IntSetting setting) {
            super(setting.settingName(),
                    ImmutableSettings.builder().put(setting.settingName(), setting.defaultValue()).build());
            this.setting = setting;
        }

        private void validate(long num) {
            if (num < setting.minValue() || num > setting.maxValue()) {
                throw invalidException();
            }
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            Number num;
            try {
                num = ExpressionToNumberVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }

            if (num == null) {
                throw new IllegalArgumentException(String.format(
                        "'%s' does not support null values", name));
            }
            int value = num.intValue();
            validate(value);
            settingsBuilder.put(this.name, value);
        }
    }

    public static class FloatSettingsApplier extends AbstractSettingsApplier {

        private final FloatSetting setting;

        public FloatSettingsApplier(FloatSetting setting) {
            super(setting.settingName(),
                    ImmutableSettings.builder().put(setting.settingName(), setting.defaultValue()).build());
            this.setting = setting;
        }

        private void validate(float num) {
            if (num < setting.minValue() || num > setting.maxValue()) {
                throw invalidException();
            }
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            Number num;
            try {
                num = ExpressionToNumberVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }

            if (num == null) {
                throw new IllegalArgumentException(String.format(
                        "'%s' does not support null values", name));
            }
            float value = num.floatValue();
            validate(value);
            settingsBuilder.put(this.name, value);
        }
    }

    public static class DoubleSettingsApplier extends AbstractSettingsApplier {

        private final DoubleSetting setting;

        public DoubleSettingsApplier(DoubleSetting setting) {
            super(setting.settingName(),
                    ImmutableSettings.builder().put(setting.settingName(), setting.defaultValue()).build());
            this.setting = setting;
        }

        private void validate(double num) {
            if (num < setting.minValue() || num > setting.maxValue()) {
                throw invalidException();
            }
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            Number num;
            try {
                num = ExpressionToNumberVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }

            if (num == null) {
                throw new IllegalArgumentException(String.format(
                        "'%s' does not support null values", name));
            }
            double value = num.doubleValue();
            validate(value);
            settingsBuilder.put(this.name, value);
        }
    }

    public static class ByteSizeSettingsApplier extends AbstractSettingsApplier {

        private final ByteSizeSetting setting;

        public ByteSizeSettingsApplier(ByteSizeSetting setting) {
            super(setting.settingName(),
                    ImmutableSettings.builder().put(setting.settingName(), setting.defaultValue()).build());
            this.setting = setting;
        }

        private void validate(long num) {
            if (num < setting.minValue() || num > setting.maxValue()) {
                throw invalidException();
            }
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            ByteSizeValue byteSizeValue;
            try {
                byteSizeValue = ExpressionToByteSizeValueVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }

            if (byteSizeValue == null) {
                throw new IllegalArgumentException(String.format(
                        "'%s' does not support null values", name));
            }
            long value = byteSizeValue.bytes();
            validate(value);
            settingsBuilder.put(this.name, value);
        }
    }

    public static class StringSettingsApplier extends AbstractSettingsApplier {

        private final StringSetting setting;

        public StringSettingsApplier(StringSetting setting) {
            super(setting.settingName(),
                    ImmutableSettings.builder().put(setting.settingName(), setting.defaultValue()).build());
            this.setting = setting;
        }

        private void validate(String value) {
            String validation = this.setting.validate(value);
            if (validation != null) {
                throw new IllegalArgumentException(validation);
            }
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            String value;
            try {
                value = ExpressionToStringVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            validate(value);
            settingsBuilder.put(name, value);
        }
    }

    public static class TimeSettingsApplier extends AbstractSettingsApplier {

        private final TimeSetting setting;

        public TimeSettingsApplier(TimeSetting setting) {
            super(setting.settingName(),
                    ImmutableSettings.builder().put(setting.settingName(), setting.defaultValue()).build());
            this.setting = setting;
        }

        private void validate(double value) {
            if (value < setting.minValue().getMillis() || value > setting.maxValue().getMillis()) {
                throw invalidException();
            }
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            TimeValue time;
            try {
                time = ExpressionToTimeValueVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            long value = time.millis();
            validate(value);
            settingsBuilder.put(name, value);
        }
    }
}

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

import com.google.common.base.Joiner;
import io.crate.analyze.expressions.*;
import io.crate.metadata.settings.*;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.types.BooleanType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Locale;
import java.util.Map;

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

        public void applyValue(ImmutableSettings.Builder settingsBuilder, Object value) {
            try {
                settingsBuilder.put(name, validate(value));
            } catch (InvalidSettingValueContentException e) {
                throw new IllegalArgumentException(e.getMessage());
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
        }

        protected Object validate(Object value) {
            return value;
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

    public static abstract class NumberSettingsApplier extends AbstractSettingsApplier {

        protected final Setting setting;

        public NumberSettingsApplier(Setting setting) {
            super(setting.settingName(),
                    ImmutableSettings.builder().put(setting.settingName(), setting.defaultValue()).build());
            this.setting = setting;
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            try {
                applyValue(settingsBuilder, ExpressionToNumberVisitor.convert(expression, parameters));
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
        }

    }

    public static class ObjectSettingsApplier extends AbstractSettingsApplier {

        public ObjectSettingsApplier(NestedSetting settings) {
            super(settings.settingName(),
                    ImmutableSettings.builder().put(settings.settingName(), settings.defaultValue()).build());
        }

        static final Joiner dotJoiner = Joiner.on('.');

        protected void flattenSettings(ImmutableSettings.Builder settingsBuilder,
                                                      String key, Object value){
            if(value instanceof Map){
                for(Map.Entry<String, Object> setting : ((Map<String, Object>) value).entrySet()){
                    flattenSettings(settingsBuilder, dotJoiner.join(key, setting.getKey()),
                            setting.getValue());
                }
            } else {
                SettingsApplier settingsApplier = CrateSettings.getSetting(key);
                if (settingsApplier == null) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH, "setting '%s' not supported", key));
                }
                settingsApplier.applyValue(settingsBuilder, value);
            }
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            Object value = null;
            try {
                value = ExpressionToObjectVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            if (!(value instanceof Map)) {
                throw new IllegalArgumentException(
                        String.format("Only object values are allowed at '%s'", name));
            }
            flattenSettings(settingsBuilder, this.name, value);
        }

        @Override
        public void applyValue(ImmutableSettings.Builder settingsBuilder, Object value) {
            throw new IllegalArgumentException(
                    String.format("Only object values are allowed at '%s'", name));
        }
    }

    public static class BooleanSettingsApplier extends AbstractSettingsApplier {

        public BooleanSettingsApplier(BoolSetting setting) {
            super(setting.settingName(),
                    ImmutableSettings.builder().put(setting.settingName(), setting.defaultValue()).build());
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            try {
                applyValue(settingsBuilder, ExpressionToObjectVisitor.convert(expression, parameters));
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
        }

        @Override
        protected Object validate(Object value) {
            return BooleanType.INSTANCE.value(value);
        }
    }

    public static class IntSettingsApplier extends NumberSettingsApplier {

        public IntSettingsApplier(IntSetting setting) {
            super(setting);
        }

        @Override
        protected Object validate(Object value) {
            Integer convertedValue = IntegerType.INSTANCE.value(value);
            IntSetting setting = (IntSetting) this.setting;
            if (convertedValue < setting.minValue() || convertedValue > setting.maxValue()) {
                throw invalidException();
            }
            return convertedValue;
        }
    }

    public static class FloatSettingsApplier extends NumberSettingsApplier {

        public FloatSettingsApplier(FloatSetting setting) {
            super(setting);
        }

        @Override
        protected Object validate(Object value) {
            Float convertedValue = FloatType.INSTANCE.value(value);
            FloatSetting setting = (FloatSetting) this.setting;
            if (convertedValue < setting.minValue() || convertedValue > setting.maxValue()) {
                throw invalidException();
            }
            return convertedValue;
        }
    }

    public static class DoubleSettingsApplier extends NumberSettingsApplier {

        public DoubleSettingsApplier(DoubleSetting setting) {
            super(setting);
        }

        @Override
        protected Object validate(Object value) {
            Double convertedValue = DoubleType.INSTANCE.value(value);
            DoubleSetting setting = (DoubleSetting) this.setting;
            if (convertedValue < setting.minValue() || convertedValue > setting.maxValue()) {
                throw invalidException();
            }
            return convertedValue;
        }
    }

    public static class ByteSizeSettingsApplier extends AbstractSettingsApplier {

        private final ByteSizeSetting setting;

        public ByteSizeSettingsApplier(ByteSizeSetting setting) {
            super(setting.settingName(),
                    ImmutableSettings.builder().put(setting.settingName(), setting.defaultValue()).build());
            this.setting = setting;
        }

        private long validate(long num) {
            if (num < setting.minValue() || num > setting.maxValue()) {
                throw invalidException();
            }
            return num;
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

            applyValue(settingsBuilder, byteSizeValue.bytes());
        }

        public void applyValue(ImmutableSettings.Builder settingsBuilder, long value) {
            settingsBuilder.put(this.name, validate(value));
        }

        @Override
        public void applyValue(ImmutableSettings.Builder settingsBuilder, Object value) {
            ByteSizeValue byteSizeValue;
            if (value instanceof Number) {
                byteSizeValue = new ByteSizeValue(((Number) value).longValue());
            } else if (value instanceof String) {
                try {
                    byteSizeValue = ByteSizeValue.parseBytesSizeValue((String) value,
                            ExpressionToByteSizeValueVisitor.DEFAULT_VALUE);
                } catch (ElasticsearchParseException e) {
                    throw invalidException(e);
                }
            } else {
                throw invalidException();
            }
            applyValue(settingsBuilder, byteSizeValue.bytes());
        }
    }

    public static class StringSettingsApplier extends AbstractSettingsApplier {

        private final StringSetting setting;

        public StringSettingsApplier(StringSetting setting) {
            super(setting.settingName(),
                    ImmutableSettings.builder().put(setting.settingName(), setting.defaultValue()).build());
            this.setting = setting;
        }


        @Override
        protected Object validate(Object value) {
            String validation = this.setting.validate((String) value);
            if (validation != null) {
                throw new InvalidSettingValueContentException(validation);
            }
            return value;
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            if (expression instanceof ObjectLiteral) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Object values are not allowed at '%s'", name));
            }
            applyValue(settingsBuilder, ExpressionToStringVisitor.convert(expression, parameters));
        }
    }

    public static class TimeSettingsApplier extends AbstractSettingsApplier {

        private final TimeSetting setting;

        public TimeSettingsApplier(TimeSetting setting) {
            super(setting.settingName(),
                    ImmutableSettings.builder().put(setting.settingName(), setting.defaultValue()).build());
            this.setting = setting;
        }

        private long validate(long value) {
            if (value < setting.minValue().getMillis() || value > setting.maxValue().getMillis()) {
                throw invalidException();
            }
            return value;
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder, Object[] parameters, Expression expression) {
            TimeValue time;
            try {
                time = ExpressionToTimeValueVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            applyValue(settingsBuilder, time.millis());
        }

        public void applyValue(ImmutableSettings.Builder settingsBuilder, long value) {
            settingsBuilder.put(name, validate(value));
        }

        @Override
        public void applyValue(ImmutableSettings.Builder settingsBuilder, Object value) {
            TimeValue timeValue;
            if (value instanceof String) {
                try {
                    timeValue = TimeValue.parseTimeValue((String) value, ExpressionToTimeValueVisitor.DEFAULT_VALUE);
                } catch (ElasticsearchParseException e) {
                    throw invalidException(e);
                }

            } else if (value instanceof Number) {
                timeValue = new TimeValue(((Number) value).longValue());
            } else {
                throw invalidException();
            }
            applyValue(settingsBuilder, timeValue.millis());
        }
    }

    static class InvalidSettingValueContentException extends IllegalArgumentException {
        InvalidSettingValueContentException(String message) {
            super(message);
        }
    }
}

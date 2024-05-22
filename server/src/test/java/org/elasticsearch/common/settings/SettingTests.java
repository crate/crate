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
package org.elasticsearch.common.settings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.AbstractScopedSettings.SettingUpdater;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.common.collections.Tuple;
import io.crate.common.unit.TimeValue;
import io.crate.types.DataTypes;

public class SettingTests extends ESTestCase {

    @Test
    public void testGet() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope);
        assertThat(booleanSetting.get(Settings.EMPTY)).isFalse();
        assertThat(booleanSetting.get(Settings.builder().put("foo.bar", false).build())).isFalse();
        assertThat(booleanSetting.get(Settings.builder().put("foo.bar", true).build())).isTrue();
    }

    @Test
    public void testByteSizeSetting() {
        final Setting<ByteSizeValue> byteSizeValueSetting =
                Setting.byteSizeSetting("a.byte.size", new ByteSizeValue(1024), Property.Dynamic, Property.NodeScope);
        assertThat(byteSizeValueSetting.isGroupSetting()).isFalse();
        final ByteSizeValue byteSizeValue = byteSizeValueSetting.get(Settings.EMPTY);
        assertThat(byteSizeValue.getBytes()).isEqualTo(1024L);
    }

    @Test
    public void testByteSizeSettingMinValue() {
        final Setting<ByteSizeValue> byteSizeValueSetting =
                Setting.byteSizeSetting(
                        "a.byte.size",
                        new ByteSizeValue(100, ByteSizeUnit.MB),
                        new ByteSizeValue(20_000_000, ByteSizeUnit.BYTES),
                        new ByteSizeValue(Integer.MAX_VALUE, ByteSizeUnit.BYTES));
        final long value = 20_000_000 - randomIntBetween(1, 1024);
        final Settings settings = Settings.builder().put("a.byte.size", value + "b").build();
        final String expectedMessage = "failed to parse value [" + value + "b] for setting [a.byte.size], must be >= [20000000b]";
        assertThatThrownBy(() -> byteSizeValueSetting.get(settings))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(expectedMessage);
    }

    @Test
    public void testByteSizeSettingMaxValue() {
        final Setting<ByteSizeValue> byteSizeValueSetting =
                Setting.byteSizeSetting(
                        "a.byte.size",
                        new ByteSizeValue(100, ByteSizeUnit.MB),
                        new ByteSizeValue(16, ByteSizeUnit.MB),
                        new ByteSizeValue(Integer.MAX_VALUE, ByteSizeUnit.BYTES));
        final long value = (1L << 31) - 1 + randomIntBetween(1, 1024);
        final Settings settings = Settings.builder().put("a.byte.size", value + "b").build();
        final String expectedMessage = "failed to parse value [" + value + "b] for setting [a.byte.size], must be <= [2147483647b]";
        assertThatThrownBy(() -> byteSizeValueSetting.get(settings))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(expectedMessage);
    }

    public void testByteSizeSettingValidation() {
        final Setting<ByteSizeValue> byteSizeValueSetting =
                Setting.byteSizeSetting("a.byte.size", s -> "2048b", Property.Dynamic, Property.NodeScope);
        final ByteSizeValue byteSizeValue = byteSizeValueSetting.get(Settings.EMPTY);
        assertThat(byteSizeValue.getBytes()).isEqualTo(2048L);
        AtomicReference<ByteSizeValue> value = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<ByteSizeValue> settingUpdater = byteSizeValueSetting.newUpdater(value::set, logger);
        assertThat(settingUpdater.apply(Settings.builder().put("a.byte.size", "12").build(), Settings.EMPTY)).isTrue();
        assertThat(value.get()).isEqualTo(new ByteSizeValue(12));
        assertThat(settingUpdater.apply(Settings.builder().put("a.byte.size", "12b").build(), Settings.EMPTY)).isTrue();
        assertThat(value.get()).isEqualTo(new ByteSizeValue(12));
    }

    @Test
    public void testMemorySize() {
        Setting<ByteSizeValue> memorySizeValueSetting = Setting.memorySizeSetting("a.byte.size", new ByteSizeValue(1024), Property.Dynamic,
                Property.NodeScope);

        assertThat(memorySizeValueSetting.isGroupSetting()).isFalse();
        ByteSizeValue memorySizeValue = memorySizeValueSetting.get(Settings.EMPTY);
        assertThat(1024).isEqualTo(memorySizeValue.getBytes());

        memorySizeValueSetting = Setting.memorySizeSetting("a.byte.size", s -> "2048b", Property.Dynamic, Property.NodeScope);
        memorySizeValue = memorySizeValueSetting.get(Settings.EMPTY);
        assertThat(2048).isEqualTo(memorySizeValue.getBytes());

        memorySizeValueSetting = Setting.memorySizeSetting("a.byte.size", "50%", Property.Dynamic, Property.NodeScope);
        assertThat(memorySizeValueSetting.isGroupSetting()).isFalse();
        memorySizeValue = memorySizeValueSetting.get(Settings.EMPTY);
        assertThat(memorySizeValue.getBytes()).isEqualTo((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.5));

        memorySizeValueSetting = Setting.memorySizeSetting("a.byte.size", s -> "25%", Property.Dynamic, Property.NodeScope);
        memorySizeValue = memorySizeValueSetting.get(Settings.EMPTY);
        assertThat(memorySizeValue.getBytes()).isEqualTo((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.25));

        AtomicReference<ByteSizeValue> value = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<ByteSizeValue> settingUpdater = memorySizeValueSetting.newUpdater(value::set, logger);

        assertThat(settingUpdater.apply(Settings.builder().put("a.byte.size", "12").build(), Settings.EMPTY)).isTrue();
        assertThat(value.get()).isEqualTo(new ByteSizeValue(12));

        assertThat(settingUpdater.apply(Settings.builder().put("a.byte.size", "12b").build(), Settings.EMPTY)).isTrue();
        assertThat(value.get()).isEqualTo(new ByteSizeValue(12));

        assertThat(settingUpdater.apply(Settings.builder().put("a.byte.size", "20%").build(), Settings.EMPTY)).isTrue();
        assertThat(value.get()).isEqualTo(new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.2)));
    }

    @Test
    public void testSimpleUpdate() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope);
        AtomicReference<Boolean> atomicBoolean = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<Boolean> settingUpdater = booleanSetting.newUpdater(atomicBoolean::set, logger);
        Settings build = Settings.builder().put("foo.bar", false).build();
        settingUpdater.apply(build, Settings.EMPTY);
        assertNull(atomicBoolean.get());
        build = Settings.builder().put("foo.bar", true).build();
        settingUpdater.apply(build, Settings.EMPTY);
        assertThat(atomicBoolean.get()).isTrue();

        // try update bogus value
        build = Settings.builder().put("foo.bar", "I am not a boolean").build();
        try {
            settingUpdater.apply(build, Settings.EMPTY);
            fail("not a boolean");
        } catch (IllegalArgumentException ex) {
            assertThat(ex, hasToString(containsString("illegal value can't update [foo.bar] from [false] to [I am not a boolean]")));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause()).isExactlyInstanceOf(IllegalArgumentException.class);
            final IllegalArgumentException cause = (IllegalArgumentException) ex.getCause();
            assertThat(
                    cause,
                    hasToString(containsString("Failed to parse value [I am not a boolean] as only [true] or [false] are allowed.")));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33135")
    public void testValidateStringSetting() {
        Settings settings = Settings.builder().putList("foo.bar", Arrays.asList("bla-a", "bla-b")).build();
        Setting<String> stringSetting = Setting.simpleString("foo.bar", Property.NodeScope);
        assertThatThrownBy(() -> stringSetting.get(settings))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Found list type value for setting [foo.bar] but but did not expect a list for it.");
    }

    private static final Setting<String> FOO_BAR_SETTING = new Setting<>(
            "foo.bar",
            "foobar",
            Function.identity(),
            new FooBarValidator(),
            DataTypes.STRING,
            Property.Dynamic,
            Property.NodeScope);

    private static final Setting<String> BAZ_QUX_SETTING = Setting.simpleString("baz.qux", Property.NodeScope);
    private static final Setting<String> QUUX_QUUZ_SETTING = Setting.simpleString("quux.quuz", Property.NodeScope);

    static class FooBarValidator implements Setting.Validator<String> {

        public static boolean invokedInIsolation;
        public static boolean invokedWithDependencies;

        @Override
        public void validate(final String value) {
            invokedInIsolation = true;
            assertThat(value).isEqualTo("foo.bar value");
        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            invokedWithDependencies = true;
            assertThat(settings.keySet().contains(BAZ_QUX_SETTING)).isTrue();
            assertThat(settings.get(BAZ_QUX_SETTING)).isEqualTo("baz.qux value");
            assertThat(settings.keySet().contains(QUUX_QUUZ_SETTING)).isTrue();
            assertThat(settings.get(QUUX_QUUZ_SETTING)).isEqualTo("quux.quuz value");
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Arrays.asList(BAZ_QUX_SETTING, QUUX_QUUZ_SETTING);
            return settings.iterator();
        }
    }

    // the purpose of this test is merely to ensure that a validator is invoked with the appropriate values
    @Test
    public void testValidator() {
        final Settings settings = Settings.builder()
                .put("foo.bar", "foo.bar value")
                .put("baz.qux", "baz.qux value")
                .put("quux.quuz", "quux.quuz value")
                .build();
        FOO_BAR_SETTING.get(settings);
        assertThat(FooBarValidator.invokedInIsolation).isTrue();
        assertThat(FooBarValidator.invokedWithDependencies).isTrue();
    }

    @Test
    public void testUpdateNotDynamic() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.NodeScope);
        assertThat(booleanSetting.isGroupSetting()).isFalse();
        AtomicReference<Boolean> atomicBoolean = new AtomicReference<>(null);
        try {
            booleanSetting.newUpdater(atomicBoolean::set, logger);
            fail("not dynamic");
        } catch (IllegalStateException ex) {
            assertThat(ex.getMessage()).isEqualTo("setting [foo.bar] is not dynamic");
        }
    }

    @Test
    public void testUpdaterIsIsolated() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope);
        AtomicReference<Boolean> ab1 = new AtomicReference<>(null);
        AtomicReference<Boolean> ab2 = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<Boolean> settingUpdater = booleanSetting.newUpdater(ab1::set, logger);
        settingUpdater.apply(Settings.builder().put("foo.bar", true).build(), Settings.EMPTY);
        assertThat(ab1.get()).isTrue();
        assertNull(ab2.get());
    }

    @Test
    public void testDefault() {
        TimeValue defaultValue = TimeValue.timeValueMillis(randomIntBetween(0, 1000000));
        Setting<TimeValue> setting =
            Setting.positiveTimeSetting("my.time.value", defaultValue, Property.NodeScope);
        assertThat(setting.isGroupSetting()).isFalse();
        String aDefault = setting.getDefaultRaw(Settings.EMPTY);
        assertThat(aDefault).isEqualTo(defaultValue.millis() + "ms");
        assertThat(setting.get(Settings.EMPTY).millis()).isEqualTo(defaultValue.millis());
        assertThat(setting.getDefault(Settings.EMPTY)).isEqualTo(defaultValue);

        Setting<String> secondaryDefault =
            new Setting<>("foo.bar", (s) -> s.get("old.foo.bar", "some_default"), Function.identity(), DataTypes.STRING, Property.NodeScope);
        assertThat(secondaryDefault.get(Settings.EMPTY)).isEqualTo("some_default");
        assertThat(secondaryDefault.get(Settings.builder().put("old.foo.bar", 42).build())).isEqualTo("42");

        Setting<String> secondaryDefaultViaSettings =
            new Setting<>("foo.bar", secondaryDefault, Function.identity(), DataTypes.STRING, Property.NodeScope);
        assertThat(secondaryDefaultViaSettings.get(Settings.EMPTY)).isEqualTo("some_default");
        assertThat(secondaryDefaultViaSettings.get(Settings.builder().put("old.foo.bar", 42).build())).isEqualTo("42");

        // It gets more complicated when there are two settings objects....
        Settings hasFallback = Settings.builder().put("foo.bar", "o").build();
        Setting<String> fallsback =
                new Setting<>("foo.baz", secondaryDefault, Function.identity(), DataTypes.STRING, Property.NodeScope);
        assertThat(fallsback.get(hasFallback)).isEqualTo("o");
        assertThat(fallsback.get(Settings.EMPTY)).isEqualTo("some_default");
        assertThat(fallsback.get(Settings.EMPTY, Settings.EMPTY)).isEqualTo("some_default");
        assertThat(fallsback.get(Settings.EMPTY, hasFallback)).isEqualTo("o");
        assertThat(fallsback.get(hasFallback, Settings.EMPTY)).isEqualTo("o");
        assertThat(fallsback.get(
                Settings.builder().put("foo.bar", "a").build(),
                Settings.builder().put("foo.bar", "b").build())).isEqualTo("a");
    }

    @Test
    public void testComplexType() {
        AtomicReference<ComplexType> ref = new AtomicReference<>(null);
        Setting<ComplexType> setting = new Setting<>("foo.bar", (s) -> "", (s) -> new ComplexType(s),
            DataTypes.STRING, Property.Dynamic, Property.NodeScope);
        assertThat(setting.isGroupSetting()).isFalse();
        ref.set(setting.get(Settings.EMPTY));
        ComplexType type = ref.get();
        ClusterSettings.SettingUpdater<ComplexType> settingUpdater = setting.newUpdater(ref::set, logger);
        assertThat(settingUpdater.apply(Settings.EMPTY, Settings.EMPTY)).isFalse();
        assertSame("no update - type has not changed", type, ref.get());

        // change from default
        assertThat(settingUpdater.apply(Settings.builder().put("foo.bar", "2").build(), Settings.EMPTY)).isTrue();
        assertNotSame("update - type has changed", type, ref.get());
        assertThat(ref.get().foo).isEqualTo("2");


        // change back to default...
        assertThat(settingUpdater.apply(Settings.EMPTY, Settings.builder().put("foo.bar", "2").build())).isTrue();
        assertNotSame("update - type has changed", type, ref.get());
        assertThat(ref.get().foo).isEqualTo("");
    }

    @Test
    public void testType() {
        Setting<Integer> integerSetting = Setting.intSetting("foo.int.bar", 1, Property.Dynamic, Property.NodeScope);
        assertThat(integerSetting.hasNodeScope()).isTrue();
        assertThat(integerSetting.hasIndexScope()).isFalse();
        integerSetting = Setting.intSetting("foo.int.bar", 1, Property.Dynamic, Property.IndexScope);
        assertThat(integerSetting.hasIndexScope()).isTrue();
        assertThat(integerSetting.hasNodeScope()).isFalse();
    }

    @Test
    public void testGroups() {
        AtomicReference<Settings> ref = new AtomicReference<>(null);
        Setting<Settings> setting = Setting.groupSetting("foo.bar.", Property.Dynamic, Property.NodeScope);
        assertThat(setting.isGroupSetting()).isTrue();
        ClusterSettings.SettingUpdater<Settings> settingUpdater = setting.newUpdater(ref::set, logger);

        Settings currentInput = Settings.builder()
                .put("foo.bar.1.value", "1")
                .put("foo.bar.2.value", "2")
                .put("foo.bar.3.value", "3").build();
        Settings previousInput = Settings.EMPTY;
        assertThat(settingUpdater.apply(currentInput, previousInput)).isTrue();
        assertNotNull(ref.get());
        Settings settings = ref.get();
        Map<String, Settings> asMap = settings.getAsGroups();
        assertThat(asMap.size()).isEqualTo(3);
        assertThat("1").isEqualTo(asMap.get("1").get("value"));
        assertThat("2").isEqualTo(asMap.get("2").get("value"));
        assertThat("3").isEqualTo(asMap.get("3").get("value"));

        previousInput = currentInput;
        currentInput = Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").put("foo.bar.3.value", "3").build();
        Settings current = ref.get();
        assertThat(settingUpdater.apply(currentInput, previousInput)).isFalse();
        assertSame(current, ref.get());

        previousInput = currentInput;
        currentInput = Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").build();
        // now update and check that we got it
        assertThat(settingUpdater.apply(currentInput, previousInput)).isTrue();
        assertNotSame(current, ref.get());

        asMap = ref.get().getAsGroups();
        assertThat(asMap.size()).isEqualTo(2);
        assertThat("1").isEqualTo(asMap.get("1").get("value"));
        assertThat("2").isEqualTo(asMap.get("2").get("value"));

        previousInput = currentInput;
        currentInput = Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "4").build();
        // now update and check that we got it
        assertThat(settingUpdater.apply(currentInput, previousInput)).isTrue();
        assertNotSame(current, ref.get());

        asMap = ref.get().getAsGroups();
        assertThat(asMap.size()).isEqualTo(2);
        assertThat("1").isEqualTo(asMap.get("1").get("value"));
        assertThat("4").isEqualTo(asMap.get("2").get("value"));

        assertThat(setting.match("foo.bar.baz")).isTrue();
        assertThat(setting.match("foo.baz.bar")).isFalse();

        ClusterSettings.SettingUpdater<Settings> predicateSettingUpdater = setting.newUpdater(ref::set, logger,(s) -> assertFalse(true));
        try {
            predicateSettingUpdater.apply(Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").build(),
                    Settings.EMPTY);
            fail("not accepted");
        } catch (IllegalArgumentException ex) {
            assertThat("illegal value can't update [foo.bar.] from [{}] to [{\"1.value\":\"1\",\"2.value\":\"2\"}]").isEqualTo(ex.getMessage());
        }
    }

    public static class ComplexType {

        final String foo;

        public ComplexType(String foo) {
            this.foo = foo;
        }
    }

    public static class Composite {

        private Integer b;
        private Integer a;

        public void set(Integer a, Integer b) {
            this.a = a;
            this.b = b;
        }

        public void validate(Integer a, Integer b) {
            if (Integer.signum(a) != Integer.signum(b)) {
                throw new IllegalArgumentException("boom");
            }
        }
    }


    @Test
    public void testComposite() {
        Composite c = new Composite();
        Setting<Integer> a = Setting.intSetting("foo.int.bar.a", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> b = Setting.intSetting("foo.int.bar.b", 1, Property.Dynamic, Property.NodeScope);
        ClusterSettings.SettingUpdater<Tuple<Integer, Integer>> settingUpdater = Setting.compoundUpdater(c::set, c::validate, a, b, logger);
        assertThat(settingUpdater.apply(Settings.EMPTY, Settings.EMPTY)).isFalse();
        assertNull(c.a);
        assertNull(c.b);

        Settings build = Settings.builder().put("foo.int.bar.a", 2).build();
        assertThat(settingUpdater.apply(build, Settings.EMPTY)).isTrue();
        assertThat(c.a.intValue()).isEqualTo(2);
        assertThat(c.b.intValue()).isEqualTo(1);

        Integer aValue = c.a;
        assertThat(settingUpdater.apply(build, build)).isFalse();
        assertSame(aValue, c.a);
        Settings previous = build;
        build = Settings.builder().put("foo.int.bar.a", 2).put("foo.int.bar.b", 5).build();
        assertThat(settingUpdater.apply(build, previous)).isTrue();
        assertThat(c.a.intValue()).isEqualTo(2);
        assertThat(c.b.intValue()).isEqualTo(5);

        // reset to default
        assertThat(settingUpdater.apply(Settings.EMPTY, build)).isTrue();
        assertThat(c.a.intValue()).isEqualTo(1);
        assertThat(c.b.intValue()).isEqualTo(1);

    }

    @Test
    public void testCompositeValidator() {
        Composite c = new Composite();
        Setting<Integer> a = Setting.intSetting("foo.int.bar.a", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> b = Setting.intSetting("foo.int.bar.b", 1, Property.Dynamic, Property.NodeScope);
        ClusterSettings.SettingUpdater<Tuple<Integer, Integer>> settingUpdater = Setting.compoundUpdater(c::set, c::validate, a, b, logger);
        assertThat(settingUpdater.apply(Settings.EMPTY, Settings.EMPTY)).isFalse();
        assertNull(c.a);
        assertNull(c.b);

        Settings build = Settings.builder().put("foo.int.bar.a", 2).build();
        assertThat(settingUpdater.apply(build, Settings.EMPTY)).isTrue();
        assertThat(c.a.intValue()).isEqualTo(2);
        assertThat(c.b.intValue()).isEqualTo(1);

        Integer aValue = c.a;
        assertThat(settingUpdater.apply(build, build)).isFalse();
        assertSame(aValue, c.a);
        Settings previous = build;
        build = Settings.builder().put("foo.int.bar.a", 2).put("foo.int.bar.b", 5).build();
        assertThat(settingUpdater.apply(build, previous)).isTrue();
        assertThat(c.a.intValue()).isEqualTo(2);
        assertThat(c.b.intValue()).isEqualTo(5);

        Settings invalid = Settings.builder().put("foo.int.bar.a", -2).put("foo.int.bar.b", 5).build();
        assertThatThrownBy(() -> settingUpdater.apply(invalid, previous))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("boom");

        // reset to default
        assertThat(settingUpdater.apply(Settings.EMPTY, build)).isTrue();
        assertThat(c.a.intValue()).isEqualTo(1);
        assertThat(c.b.intValue()).isEqualTo(1);

    }

    @Test
    public void testListSettingsDeprecated() {
        final Setting<List<String>> deprecatedListSetting =
                Setting.listSetting(
                        "foo.deprecated",
                        Collections.singletonList("foo.deprecated"),
                        Function.identity(),
                        DataTypes.STRING_ARRAY,
                        Property.Deprecated,
                        Property.NodeScope);
        final Setting<List<String>> nonDeprecatedListSetting =
                Setting.listSetting(
                        "foo.non_deprecated", Collections.singletonList("foo.non_deprecated"), Function.identity(), DataTypes.STRING_ARRAY, Property.NodeScope);
        final Settings settings = Settings.builder()
                .put("foo.deprecated", "foo.deprecated1,foo.deprecated2")
                .put("foo.deprecated", "foo.non_deprecated1,foo.non_deprecated2")
                .build();
        deprecatedListSetting.get(settings);
        nonDeprecatedListSetting.get(settings);
        assertSettingDeprecationsAndWarnings(new Setting[]{deprecatedListSetting});
    }

    @Test
    public void testListSettings() {
        Setting<List<String>> listSetting = Setting.listSetting("foo.bar", Arrays.asList("foo,bar"), (s) -> s.toString(),
            DataTypes.STRING_ARRAY, Property.Dynamic, Property.NodeScope);
        List<String> value = listSetting.get(Settings.EMPTY);
        assertThat(listSetting.exists(Settings.EMPTY)).isFalse();
        assertThat(value.size()).isEqualTo(1);
        assertThat(value.get(0)).isEqualTo("foo,bar");

        List<String> input = Arrays.asList("test", "test1, test2", "test", ",,,,");
        Settings.Builder builder = Settings.builder().putList("foo.bar", input.toArray(new String[0]));
        assertThat(listSetting.exists(builder.build())).isTrue();
        value = listSetting.get(builder.build());
        assertThat(value.size()).isEqualTo(input.size());
        assertArrayEquals(value.toArray(new String[0]), input.toArray(new String[0]));

        // try to parse this really annoying format
        builder = Settings.builder();
        for (int i = 0; i < input.size(); i++) {
            builder.put("foo.bar." + i, input.get(i));
        }
        value = listSetting.get(builder.build());
        assertThat(value.size()).isEqualTo(input.size());
        assertArrayEquals(value.toArray(new String[0]), input.toArray(new String[0]));
        assertThat(listSetting.exists(builder.build())).isTrue();

        AtomicReference<List<String>> ref = new AtomicReference<>();
        AbstractScopedSettings.SettingUpdater<List<String>> settingUpdater = listSetting.newUpdater(ref::set, logger);
        assertThat(settingUpdater.hasChanged(builder.build(), Settings.EMPTY)).isTrue();
        settingUpdater.apply(builder.build(), Settings.EMPTY);
        assertThat(ref.get().size()).isEqualTo(input.size());
        assertArrayEquals(ref.get().toArray(new String[0]), input.toArray(new String[0]));

        settingUpdater.apply(Settings.builder().putList("foo.bar", "123").build(), builder.build());
        assertThat(ref.get().size()).isEqualTo(1);
        assertArrayEquals(ref.get().toArray(new String[0]), new String[] {"123"});

        settingUpdater.apply(Settings.builder().put("foo.bar", "1,2,3").build(), Settings.builder().putList("foo.bar", "123").build());
        assertThat(ref.get().size()).isEqualTo(3);
        assertArrayEquals(ref.get().toArray(new String[0]), new String[] {"1", "2", "3"});

        settingUpdater.apply(Settings.EMPTY, Settings.builder().put("foo.bar", "1,2,3").build());
        assertThat(ref.get().size()).isEqualTo(1);
        assertThat(ref.get().get(0)).isEqualTo("foo,bar");

        Setting<List<Integer>> otherSettings = Setting.listSetting("foo.bar", Collections.emptyList(), Integer::parseInt,
            DataTypes.INTEGER_ARRAY, Property.Dynamic, Property.NodeScope);
        List<Integer> defaultValue = otherSettings.get(Settings.EMPTY);
        assertThat(defaultValue.size()).isEqualTo(0);
        List<Integer> intValues = otherSettings.get(Settings.builder().put("foo.bar", "0,1,2,3").build());
        assertThat(intValues.size()).isEqualTo(4);
        for (int i = 0; i < intValues.size(); i++) {
            assertThat(intValues.get(i).intValue()).isEqualTo(i);
        }

        Setting<List<String>> settingWithFallback = Setting.listSetting("foo.baz", listSetting, Function.identity(),
            DataTypes.STRING_ARRAY, Property.Dynamic, Property.NodeScope);
        value = settingWithFallback.get(Settings.EMPTY);
        assertThat(value.size()).isEqualTo(1);
        assertThat(value.get(0)).isEqualTo("foo,bar");

        value = settingWithFallback.get(Settings.builder().putList("foo.bar", "1", "2").build());
        assertThat(value.size()).isEqualTo(2);
        assertThat(value.get(0)).isEqualTo("1");
        assertThat(value.get(1)).isEqualTo("2");

        value = settingWithFallback.get(Settings.builder().putList("foo.baz", "3", "4").build());
        assertThat(value.size()).isEqualTo(2);
        assertThat(value.get(0)).isEqualTo("3");
        assertThat(value.get(1)).isEqualTo("4");

        value = settingWithFallback.get(Settings.builder().putList("foo.baz", "3", "4").putList("foo.bar", "1", "2").build());
        assertThat(value.size()).isEqualTo(2);
        assertThat(value.get(0)).isEqualTo("3");
        assertThat(value.get(1)).isEqualTo("4");
    }

    @Test
    public void testListSettingAcceptsNumberSyntax() {
        Setting<List<String>> listSetting = Setting.listSetting("foo.bar", Arrays.asList("foo,bar"), (s) -> s.toString(),
            DataTypes.STRING_ARRAY, Property.Dynamic, Property.NodeScope);
        List<String> input = Arrays.asList("test", "test1, test2", "test", ",,,,");
        Settings.Builder builder = Settings.builder().putList("foo.bar", input.toArray(new String[0]));
        // try to parse this really annoying format
        for (String key : builder.keys()) {
            assertThat(listSetting.match(key)).as("key: " + key + " doesn't match").isTrue();
        }
        builder = Settings.builder().put("foo.bar", "1,2,3");
        for (String key : builder.keys()) {
            assertThat(listSetting.match(key)).as("key: " + key + " doesn't match").isTrue();
        }
        assertThat(listSetting.match("foo_bar")).isFalse();
        assertThat(listSetting.match("foo_bar.1")).isFalse();
        assertThat(listSetting.match("foo.bar")).isTrue();
        assertThat(listSetting.match("foo.bar." + randomIntBetween(0,10000))).isTrue();
    }

    @Test
    public void testDynamicKeySetting() {
        Setting<Boolean> setting = Setting.prefixKeySetting("foo.", (key) -> Setting.boolSetting(key, false, Property.NodeScope));
        assertThat(setting.hasComplexMatcher()).isTrue();
        assertThat(setting.match("foo.bar")).isTrue();
        assertThat(setting.match("foo")).isFalse();
        Setting<Boolean> concreteSetting = setting.getConcreteSetting("foo.bar");
        assertThat(concreteSetting.get(Settings.builder().put("foo.bar", "true").build())).isTrue();
        assertThat(concreteSetting.get(Settings.builder().put("foo.baz", "true").build())).isFalse();

        try {
            setting.getConcreteSetting("foo");
            fail();
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage()).isEqualTo("key [foo] must match [foo.] but didn't.");
        }
    }

    @Test
    public void testAffixKeySetting() {
        Setting<Boolean> setting =
            Setting.affixKeySetting("foo.", "enable", (key) -> Setting.boolSetting(key, false, Property.NodeScope));
        assertThat(setting.hasComplexMatcher()).isTrue();
        assertThat(setting.match("foo.bar.enable")).isTrue();
        assertThat(setting.match("foo.baz.enable")).isTrue();
        assertThat(setting.match("foo.bar.baz.enable")).isFalse();
        assertThat(setting.match("foo.bar")).isFalse();
        assertThat(setting.match("foo.bar.baz.enabled")).isFalse();
        assertThat(setting.match("foo")).isFalse();
        Setting<Boolean> concreteSetting = setting.getConcreteSetting("foo.bar.enable");
        assertThat(concreteSetting.get(Settings.builder().put("foo.bar.enable", "true").build())).isTrue();
        assertThat(concreteSetting.get(Settings.builder().put("foo.baz.enable", "true").build())).isFalse();

        assertThatThrownBy(() -> setting.getConcreteSetting("foo"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("key [foo] must match [foo.*.enable] but didn't.");

        assertThatThrownBy(() -> Setting.affixKeySetting("foo", "enable",
            (key) -> Setting.boolSetting(key, false, Property.NodeScope))
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("prefix must end with a '.'");

        Setting<List<String>> listAffixSetting = Setting.affixKeySetting("foo.", "bar",
            (key) -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), DataTypes.STRING_ARRAY, Property.NodeScope));

        assertThat(listAffixSetting.hasComplexMatcher()).isTrue();
        assertThat(listAffixSetting.match("foo.test.bar")).isTrue();
        assertThat(listAffixSetting.match("foo.test_1.bar")).isTrue();
        assertThat(listAffixSetting.match("foo.buzz.baz.bar")).isFalse();
        assertThat(listAffixSetting.match("foo.bar")).isFalse();
        assertThat(listAffixSetting.match("foo.baz")).isFalse();
        assertThat(listAffixSetting.match("foo")).isFalse();
    }

    @Test
    public void testAffixSettingNamespaces() {
        Setting.AffixSetting<Boolean> setting =
            Setting.affixKeySetting("foo.", "enable", (key) -> Setting.boolSetting(key, false, Property.NodeScope));
        Settings build = Settings.builder()
            .put("foo.bar.enable", "true")
            .put("foo.baz.enable", "true")
            .put("foo.boom.enable", "true")
            .put("something.else", "true")
            .build();
        Set<String> namespaces = setting.getNamespaces(build);
        assertThat(namespaces.size()).isEqualTo(3);
        assertThat(namespaces.contains("bar")).isTrue();
        assertThat(namespaces.contains("baz")).isTrue();
        assertThat(namespaces.contains("boom")).isTrue();
    }

    @Test
    public void testAffixAsMap() {
        Setting.AffixSetting<String> setting = Setting.prefixKeySetting("foo.bar.", key ->
            Setting.simpleString(key, Property.NodeScope));
        Settings build = Settings.builder().put("foo.bar.baz", 2).put("foo.bar.foobar", 3).build();
        Map<String, String> asMap = setting.getAsMap(build);
        assertThat(asMap.size()).isEqualTo(2);
        assertThat(asMap.get("baz")).isEqualTo("2");
        assertThat(asMap.get("foobar")).isEqualTo("3");

        setting = Setting.prefixKeySetting("foo.bar.", key ->
            Setting.simpleString(key, Property.NodeScope));
        build = Settings.builder().put("foo.bar.baz", 2).put("foo.bar.foobar", 3).put("foo.bar.baz.deep", 45).build();
        asMap = setting.getAsMap(build);
        assertThat(asMap.size()).isEqualTo(3);
        assertThat(asMap.get("baz")).isEqualTo("2");
        assertThat(asMap.get("foobar")).isEqualTo("3");
        assertThat(asMap.get("baz.deep")).isEqualTo("45");
    }

    @Test
    public void testGetAllConcreteSettings() {
        Setting.AffixSetting<List<String>> listAffixSetting = Setting.affixKeySetting("foo.", "bar",
            (key) -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), DataTypes.STRING_ARRAY, Property.NodeScope));

        Settings settings = Settings.builder()
            .putList("foo.1.bar", "1", "2")
            .putList("foo.2.bar", "3", "4", "5")
            .putList("foo.bar", "6")
            .putList("some.other", "6")
            .putList("foo.3.bar", "6")
            .build();
        Stream<Setting<List<String>>> allConcreteSettings = listAffixSetting.getAllConcreteSettings(settings);
        Map<String, List<String>> collect = allConcreteSettings.collect(Collectors.toMap(Setting::getKey, (s) -> s.get(settings)));
        assertThat(collect.size()).isEqualTo(3);
        assertThat(collect.get("foo.1.bar")).isEqualTo(Arrays.asList("1", "2"));
        assertThat(collect.get("foo.2.bar")).isEqualTo(Arrays.asList("3", "4", "5"));
        assertThat(collect.get("foo.3.bar")).isEqualTo(Arrays.asList("6"));
    }

    @Test
    public void testAffixSettingsFailOnGet() {
        Setting.AffixSetting<List<String>> listAffixSetting = Setting.affixKeySetting("foo.", "bar",
            (key) -> Setting.listSetting(key, Collections.singletonList("testelement"), Function.identity(), DataTypes.STRING_ARRAY, Property.NodeScope));
        assertThatThrownBy(() -> listAffixSetting.get(Settings.EMPTY))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> listAffixSetting.getRaw(Settings.EMPTY))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
        assertThat(listAffixSetting.getDefault(Settings.EMPTY)).isEqualTo(Collections.singletonList("testelement"));
        assertThat(listAffixSetting.getDefaultRaw(Settings.EMPTY)).isEqualTo("[\"testelement\"]");
    }

    @Test
    public void testMinMaxInt() {
        Setting<Integer> integerSetting = Setting.intSetting("foo.bar", 1, 0, 10, Property.NodeScope);
        try {
            integerSetting.get(Settings.builder().put("foo.bar", 11).build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage()).isEqualTo("Failed to parse value [11] for setting [foo.bar] must be <= 10");
        }

        try {
            integerSetting.get(Settings.builder().put("foo.bar", -1).build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage()).isEqualTo("Failed to parse value [-1] for setting [foo.bar] must be >= 0");
        }

        assertThat(integerSetting.get(Settings.builder().put("foo.bar", 5).build()).intValue()).isEqualTo(5);
        assertThat(integerSetting.get(Settings.EMPTY).intValue()).isEqualTo(1);
    }

    /**
     * Only one single scope can be added to any setting
     */
    @Test
    public void testMutuallyExclusiveScopes() {
        // Those should pass
        Setting<String> setting = Setting.simpleString("foo.bar", Property.NodeScope);
        assertThat(setting.hasNodeScope()).isTrue();
        assertThat(setting.hasIndexScope()).isFalse();
        setting = Setting.simpleString("foo.bar", Property.IndexScope);
        assertThat(setting.hasIndexScope()).isTrue();
        assertThat(setting.hasNodeScope()).isFalse();

        // We accept settings with no scope but they will be rejected when we register with SettingsModule.registerSetting
        setting = Setting.simpleString("foo.bar");
        assertThat(setting.hasIndexScope()).isFalse();
        assertThat(setting.hasNodeScope()).isFalse();

        // We accept settings with multiple scopes but they will be rejected when we register with SettingsModule.registerSetting
        setting = Setting.simpleString("foo.bar", Property.IndexScope, Property.NodeScope);
        assertThat(setting.hasIndexScope()).isTrue();
        assertThat(setting.hasNodeScope()).isTrue();
    }

    /**
     * We can't have Null properties
     */
    @Test
    public void testRejectNullProperties() {
        assertThatThrownBy(() -> Setting.simpleString("foo.bar", (Property[]) null))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("properties cannot be null for setting");
    }

    @Test
    public void testRejectConflictingDynamicAndFinalProperties() {
        assertThatThrownBy(() -> Setting.simpleString("foo.bar", Property.Final, Property.Dynamic))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("final setting [foo.bar] cannot be dynamic");
    }

    @Test
    public void testRejectNonIndexScopedNotCopyableOnResizeSetting() {
        assertThatThrownBy(() -> Setting.simpleString("foo.bar", Property.NotCopyableOnResize))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("non-index-scoped setting [foo.bar] can not have property [NotCopyableOnResize]");
    }

    @Test
    public void testRejectNonIndexScopedInternalIndexSetting() {
        assertThatThrownBy(() -> Setting.simpleString("foo.bar", Property.InternalIndex))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("non-index-scoped setting [foo.bar] can not have property [InternalIndex]");
    }

    @Test
    public void testRejectNonIndexScopedPrivateIndexSetting() {
        assertThatThrownBy(() -> Setting.simpleString("foo.bar", Property.PrivateIndex))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("non-index-scoped setting [foo.bar] can not have property [PrivateIndex]");
    }

    @Test
    public void testTimeValue() {
        final TimeValue random = TimeValue.parseTimeValue(randomTimeValue(), "test");

        Setting<TimeValue> setting = Setting.timeSetting("foo", random);
        assertThat(setting.get(Settings.EMPTY)).isEqualTo(random);

        final int factor = randomIntBetween(1, 10);
        setting = Setting.timeSetting("foo", (s) -> TimeValue.timeValueMillis(random.millis() * factor), TimeValue.ZERO);
        assertThat(setting.get(Settings.builder().put("foo", "12h").build())).isEqualTo(TimeValue.timeValueHours(12));
        assertThat(setting.get(Settings.EMPTY).millis()).isEqualTo(random.millis() * factor);
    }

    @Test
    public void testTimeValueBounds() {
        Setting<TimeValue> settingWithLowerBound
            = Setting.timeSetting("foo", TimeValue.timeValueSeconds(10), TimeValue.timeValueSeconds(5));
        assertThat(settingWithLowerBound.get(Settings.EMPTY)).isEqualTo(TimeValue.timeValueSeconds(10));

        assertThat(settingWithLowerBound.get(Settings.builder().put("foo", "5000ms").build())).isEqualTo(TimeValue.timeValueSeconds(5));
        assertThatThrownBy(() -> settingWithLowerBound.get(Settings.builder().put("foo", "4999ms").build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("failed to parse value [4999ms] for setting [foo], must be >= [5s]");

        Setting<TimeValue> settingWithBothBounds = Setting.timeSetting("bar",
            TimeValue.timeValueSeconds(10), TimeValue.timeValueSeconds(5), TimeValue.timeValueSeconds(20));
        assertThat(settingWithBothBounds.get(Settings.EMPTY)).isEqualTo(TimeValue.timeValueSeconds(10));

        assertThat(settingWithBothBounds.get(Settings.builder().put("bar", "5000ms").build())).isEqualTo(TimeValue.timeValueSeconds(5));
        assertThat(settingWithBothBounds.get(Settings.builder().put("bar", "20000ms").build())).isEqualTo(TimeValue.timeValueSeconds(20));
        assertThatThrownBy(() -> settingWithBothBounds.get(Settings.builder().put("bar", "4999ms").build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("failed to parse value [4999ms] for setting [bar], must be >= [5s]");

        assertThatThrownBy(() -> settingWithBothBounds.get(Settings.builder().put("bar", "20001ms").build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("failed to parse value [20001ms] for setting [bar], must be <= [20s]");
    }

    @Test
    public void testSettingsGroupUpdater() {
        Setting<Integer> intSetting = Setting.intSetting("prefix.foo", 1, Property.NodeScope, Property.Dynamic);
        Setting<Integer> intSetting2 = Setting.intSetting("prefix.same", 1, Property.NodeScope, Property.Dynamic);
        AbstractScopedSettings.SettingUpdater<Settings> updater = Setting.groupedSettingsUpdater(s -> {},
            Arrays.asList(intSetting, intSetting2));

        Settings current = Settings.builder().put("prefix.foo", 123).put("prefix.same", 5555).build();
        Settings previous = Settings.builder().put("prefix.foo", 321).put("prefix.same", 5555).build();
        assertThat(updater.apply(current, previous)).isTrue();
    }

    @Test
    public void testSettingsGroupUpdaterRemoval() {
        Setting<Integer> intSetting = Setting.intSetting("prefix.foo", 1, Property.NodeScope, Property.Dynamic);
        Setting<Integer> intSetting2 = Setting.intSetting("prefix.same", 1, Property.NodeScope, Property.Dynamic);
        AbstractScopedSettings.SettingUpdater<Settings> updater = Setting.groupedSettingsUpdater(s -> {},
            Arrays.asList(intSetting, intSetting2));

        Settings current = Settings.builder().put("prefix.same", 5555).build();
        Settings previous = Settings.builder().put("prefix.foo", 321).put("prefix.same", 5555).build();
        assertThat(updater.apply(current, previous)).isTrue();
    }

    @Test
    public void testSettingsGroupUpdaterWithAffixSetting() {
        Setting<Integer> intSetting = Setting.intSetting("prefix.foo", 1, Property.NodeScope, Property.Dynamic);
        Setting.AffixSetting<String> prefixKeySetting =
            Setting.prefixKeySetting("prefix.foo.bar.", key -> Setting.simpleString(key, Property.NodeScope, Property.Dynamic));
        Setting.AffixSetting<String> affixSetting =
            Setting.affixKeySetting("prefix.foo.", "suffix", key -> Setting.simpleString(key,Property.NodeScope, Property.Dynamic));

        AbstractScopedSettings.SettingUpdater<Settings> updater = Setting.groupedSettingsUpdater(s -> {},
            Arrays.asList(intSetting, prefixKeySetting, affixSetting));

        Settings.Builder currentSettingsBuilder = Settings.builder()
            .put("prefix.foo.bar.baz", "foo")
            .put("prefix.foo.infix.suffix", "foo");
        Settings.Builder previousSettingsBuilder = Settings.builder()
            .put("prefix.foo.bar.baz", "foo")
            .put("prefix.foo.infix.suffix", "foo");
        boolean removePrefixKeySetting = randomBoolean();
        boolean changePrefixKeySetting = randomBoolean();
        boolean removeAffixKeySetting = randomBoolean();
        boolean changeAffixKeySetting = randomBoolean();
        boolean removeAffixNamespace = randomBoolean();

        if (removePrefixKeySetting) {
            previousSettingsBuilder.remove("prefix.foo.bar.baz");
        }
        if (changePrefixKeySetting) {
            currentSettingsBuilder.put("prefix.foo.bar.baz", "bar");
        }
        if (removeAffixKeySetting) {
            previousSettingsBuilder.remove("prefix.foo.infix.suffix");
        }
        if (changeAffixKeySetting) {
            currentSettingsBuilder.put("prefix.foo.infix.suffix", "bar");
        }
        if (removeAffixKeySetting == false && changeAffixKeySetting == false && removeAffixNamespace) {
            currentSettingsBuilder.remove("prefix.foo.infix.suffix");
            currentSettingsBuilder.put("prefix.foo.infix2.suffix", "bar");
            previousSettingsBuilder.put("prefix.foo.infix2.suffix", "bar");
        }

        boolean expectedChange = removeAffixKeySetting || removePrefixKeySetting || changeAffixKeySetting || changePrefixKeySetting
            || removeAffixNamespace;
        assertThat(updater.apply(currentSettingsBuilder.build(), previousSettingsBuilder.build())).isEqualTo(expectedChange);
    }

    @Test
    public void testAffixNamespacesWithGroupSetting() {
        final Setting.AffixSetting<Settings> affixSetting =
            Setting.affixKeySetting("prefix.","suffix",
                (key) -> Setting.groupSetting(key + ".", Setting.Property.Dynamic, Setting.Property.NodeScope));

        assertThat(affixSetting.getNamespaces(Settings.builder().put("prefix.infix.suffix", "anything").build())).hasSize(1);
        assertThat(affixSetting.getNamespaces(Settings.builder().put("prefix.infix.suffix.anything", "anything").build())).hasSize(1);
    }

    @Test
    public void testExists() {
        final Setting<?> fooSetting = Setting.simpleString("foo", Property.NodeScope);
        assertThat(fooSetting.exists(Settings.EMPTY)).isFalse();
        assertThat(fooSetting.exists(Settings.builder().put("foo", "bar").build())).isTrue();
    }

    @Test
    public void testExistsWithFallback() {
        final int count = randomIntBetween(1, 16);
        Setting<String> current = Setting.simpleString("fallback0", Property.NodeScope);
        for (int i = 1; i < count; i++) {
            final Setting<String> next = new Setting<>(
                new Setting.SimpleKey("fallback" + i), current, Function.identity(), DataTypes.STRING, Property.NodeScope);
            current = next;
        }
        final Setting<String> fooSetting = new Setting<>(
            new Setting.SimpleKey("foo"), current, Function.identity(), DataTypes.STRING, Property.NodeScope);
        assertThat(fooSetting.exists(Settings.EMPTY)).isFalse();
        if (randomBoolean()) {
            assertThat(fooSetting.exists(Settings.builder().put("foo", "bar").build())).isTrue();
        } else {
            final String setting = "fallback" + randomIntBetween(0, count - 1);
            assertThat(fooSetting.exists(Settings.builder().put(setting, "bar").build())).isFalse();
            assertThat(fooSetting.existsOrFallbackExists(Settings.builder().put(setting, "bar").build())).isTrue();
        }
    }

    @Test
    public void testGetWithFallbackWithProvidedSettingValue() {
        Setting<String> fallbackSetting = Setting.simpleString("bar", Property.NodeScope);
        Setting<String> setting = Setting.simpleString("foo", fallbackSetting, Property.NodeScope);
        assertThat(setting.getWithFallback(Settings.builder().put("foo", "test").build())).isEqualTo("test");
    }

    @Test
    public void testGetWithFallbackWithProvidedFallbackSettingValue() {
        Setting<String> fallbackSetting = Setting.simpleString("bar", Property.NodeScope);
        Setting<String> setting = Setting.simpleString("foo", fallbackSetting, Property.NodeScope);
        assertThat(setting.getWithFallback(Settings.builder().put("bar", "test").build())).isEqualTo("test");
    }

    @Test
    public void testGetWithFallbackReturnsFallbackSettingDefaultIfNoSettingValuesProvided() {
        Setting<String> fallbackSetting = Setting.simpleString("bar", "test", Property.NodeScope);
        Setting<String> setting = Setting.simpleString("foo", fallbackSetting, Property.NodeScope);
        assertThat(setting.getWithFallback(Settings.EMPTY)).isEqualTo("test");
    }

    @Test
    public void testAffixMapUpdateWithNullSettingValue() {
        // GIVEN an affix setting changed from "prefix._foo"="bar" to "prefix._foo"=null
        final Settings current = Settings.builder()
            .put("prefix._foo", (String) null)
            .build();

        final Settings previous = Settings.builder()
            .put("prefix._foo", "bar")
            .build();

        final Setting.AffixSetting<String> affixSetting =
            Setting.prefixKeySetting("prefix" + ".",
                key -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope));

        final Consumer<Map<String, String>> consumer = (map) -> {};
        final BiConsumer<String, String> validator = (s1, s2) -> {};

        // WHEN creating an affix updater
        final SettingUpdater<Map<String, String>> updater = affixSetting.newAffixMapUpdater(consumer, logger, validator);

        // THEN affix updater is always expected to have changed (even when defaults are omitted)
        assertThat(updater.hasChanged(current, previous)).isTrue();

        // THEN changes are expected when defaults aren't omitted
        final Map<String, String> updatedSettings = updater.getValue(current, previous);
        assertNotNull(updatedSettings);
        assertThat(updatedSettings.size()).isEqualTo(1);

        // THEN changes are reported when defaults aren't omitted
        final String key = updatedSettings.keySet().iterator().next();
        final String value = updatedSettings.get(key);
        assertThat(key).isEqualTo("_foo");
        assertThat(value).isEqualTo("");
    }

    /*
     * The empty version lookup was not correctly handled and resulted in a wrong lucene version
     */
    @Test
    public void test_empty_version_from_setting() {
        var emptyVersion = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(Settings.EMPTY);
        assertThat(emptyVersion).isEqualTo((Version.V_EMPTY));
    }

    /*
     * The default version handling was broken because it was based on the external id and not the correct
     * internal id and led to invalid ids.
     */
    @Test
    public void test_default_version_from_setting() {
        var defaultVersion = Setting.versionSetting(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).get(Settings.EMPTY);
        assertThat(defaultVersion).isEqualTo(Version.CURRENT);
    }
}

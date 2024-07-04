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
import static org.assertj.core.api.Assertions.fail;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class ScopedSettingsTests extends ESTestCase {

    @Test
    public void testResetSetting() {
        Setting<Integer> dynamicSetting = Setting.intSetting("some.dyn.setting", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> staticSetting = Setting.intSetting("some.static.setting", 1, Property.NodeScope);
        Settings currentSettings = Settings.builder().put("some.dyn.setting", 5).put("some.static.setting", 6).put("archived.foo.bar", 9)
            .build();
        ClusterSettings service = new ClusterSettings(currentSettings
            , new HashSet<>(List.of(dynamicSetting, staticSetting)));

        assertThatThrownBy(() -> service.updateDynamicSettings(
                Settings.builder().put("some.dyn.setting", 8).putNull("some.static.setting").build(),
                Settings.builder().put(currentSettings), Settings.builder(), "node")
        ).isExactlyInstanceOf(IllegalArgumentException.class);

        Settings.Builder target = Settings.builder().put(currentSettings);
        Settings.Builder update = Settings.builder();
        assertThat(service.updateDynamicSettings(Settings.builder().put("some.dyn.setting", 8).build(),
            target, update, "node")).isTrue();
        assertThat(dynamicSetting.get(target.build()).intValue()).isEqualTo(8);
        assertThat(staticSetting.get(target.build()).intValue()).isEqualTo(6);
        assertThat(target.build().getAsInt("archived.foo.bar", null).intValue()).isEqualTo(9);

        target = Settings.builder().put(currentSettings);
        update = Settings.builder();
        assertThat(service.updateDynamicSettings(Settings.builder().putNull("some.dyn.setting").build(),
            target, update, "node")).isTrue();
        assertThat(dynamicSetting.get(target.build()).intValue()).isEqualTo(1);
        assertThat(staticSetting.get(target.build()).intValue()).isEqualTo(6);
        assertThat(target.build().getAsInt("archived.foo.bar", null).intValue()).isEqualTo(9);

        target = Settings.builder().put(currentSettings);
        update = Settings.builder();
        assertThat(service.updateDynamicSettings(Settings.builder().putNull("archived.foo.bar").build(),
            target, update, "node")).isTrue();
        assertThat(dynamicSetting.get(target.build()).intValue()).isEqualTo(5);
        assertThat(staticSetting.get(target.build()).intValue()).isEqualTo(6);
        assertThat(target.build().getAsInt("archived.foo.bar", null)).isNull();

        target = Settings.builder().put(currentSettings);
        update = Settings.builder();
        assertThat(service.updateDynamicSettings(Settings.builder().putNull("some.*").build(),
            target, update, "node")).isTrue();
        assertThat(dynamicSetting.get(target.build()).intValue()).isEqualTo(1);
        assertThat(staticSetting.get(target.build()).intValue()).isEqualTo(6);
        assertThat(target.build().getAsInt("archived.foo.bar", null).intValue()).isEqualTo(9);

        target = Settings.builder().put(currentSettings);
        update = Settings.builder();
        assertThat(service.updateDynamicSettings(Settings.builder().putNull("*").build(),
            target, update, "node")).isTrue();
        assertThat(dynamicSetting.get(target.build()).intValue()).isEqualTo(1);
        assertThat(staticSetting.get(target.build()).intValue()).isEqualTo(6);
        assertThat(target.build().getAsInt("archived.foo.bar", null)).isNull();
    }

    @Test
    public void testResetSettingWithIPValidator() {
        Settings currentSettings = Settings.builder().put("index.routing.allocation.require._ip", "192.168.0.1,127.0.0.1")
            .put("index.some.dyn.setting", 1)
            .build();
        Setting<Integer> dynamicSetting = Setting.intSetting("index.some.dyn.setting", 1, Property.Dynamic, Property.IndexScope);

        IndexScopedSettings settings = new IndexScopedSettings(currentSettings,
            new HashSet<>(List.of(dynamicSetting, IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING)));
        Map<String, String> s = IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getAsMap(currentSettings);
        assertThat(s.size()).isEqualTo(1);
        assertThat(s.get("_ip")).isEqualTo("192.168.0.1,127.0.0.1");
        Settings.Builder builder = Settings.builder();
        Settings updates = Settings.builder().putNull("index.routing.allocation.require._ip")
            .put("index.some.dyn.setting", 1).build();
        settings.validate(updates, false);
        settings.updateDynamicSettings(updates,
            Settings.builder().put(currentSettings), builder, "node");
        currentSettings = builder.build();
        s = IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getAsMap(currentSettings);
        assertThat(s.size()).isEqualTo(0);
        assertThat(dynamicSetting.get(currentSettings).intValue()).isEqualTo(1);
        assertThat(currentSettings.size()).isEqualTo(1);
    }

    @Test
    public void testAddConsumer() {
        Setting<Integer> testSetting = Setting.intSetting("foo.bar", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> testSetting2 = Setting.intSetting("foo.bar.baz", 1, Property.Dynamic, Property.NodeScope);
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY, Collections.singleton(testSetting));

        AtomicInteger consumer = new AtomicInteger();
        service.addSettingsUpdateConsumer(testSetting, consumer::set);
        AtomicInteger consumer2 = new AtomicInteger();
        try {
            service.addSettingsUpdateConsumer(testSetting2, consumer2::set);
            fail("setting not registered");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage()).isEqualTo("Setting is not registered for key [foo.bar.baz]");
        }

        try {
            service.addSettingsUpdateConsumer(testSetting, testSetting2, (a, b) -> {
                consumer.set(a);
                consumer2.set(b);
            });
            fail("setting not registered");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage()).isEqualTo("Setting is not registered for key [foo.bar.baz]");
        }
        assertThat(consumer.get()).isEqualTo(0);
        assertThat(consumer2.get()).isEqualTo(0);
        service.applySettings(Settings.builder().put("foo.bar", 2).put("foo.bar.baz", 15).build());
        assertThat(consumer.get()).isEqualTo(2);
        assertThat(consumer2.get()).isEqualTo(0);
    }

    @Test
    public void testDependentSettings() {
        Setting.AffixSetting<String> stringSetting = Setting.affixKeySetting("foo.", "name",
            k -> Setting.simpleString(k, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<Integer> intSetting = Setting.affixKeySetting("foo.", "bar",
            k ->  Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope), () -> stringSetting);

        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY, new HashSet<>(List.of(intSetting, stringSetting)));

        assertThatThrownBy(() -> service.validate(Settings.builder().put("foo.test.bar", 7).build(), true))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("missing required setting [foo.test.name] for setting [foo.test.bar]");

        service.validate(Settings.builder()
            .put("foo.test.name", "test")
            .put("foo.test.bar", 7)
            .build(), true);

        service.validate(Settings.builder().put("foo.test.bar", 7).build(), false);
    }

    @Test
    public void testDependentSettingsValidate() {
        Setting.AffixSetting<String> stringSetting = Setting.affixKeySetting(
            "foo.",
            "name",
            k -> Setting.simpleString(k, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<Integer> intSetting = Setting.affixKeySetting(
            "foo.",
            "bar",
            k -> Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope),
            new Setting.AffixSettingDependency() {

                @Override
                public Setting.AffixSetting<?> getSetting() {
                    return stringSetting;
                }

                @Override
                public void validate(final String key, final Object value, final Object dependency) {
                    if ("valid".equals(dependency) == false) {
                        throw new SettingsException("[" + key + "] is set but [name] is [" + dependency + "]");
                    }
                }
            });

        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY, new HashSet<>(List.of(intSetting, stringSetting)));

        assertThatThrownBy(() -> service.validate(Settings.builder().put("foo.test.bar", 7).put("foo.test.name", "invalid").build(), true))
            .isExactlyInstanceOf(SettingsException.class)
            .hasMessage("[foo.test.bar] is set but [name] is [invalid]");

        service.validate(Settings.builder()
                .put("foo.test.bar", 7)
                .put("foo.test.name", "valid")
                .build(),
            true);

        service.validate(Settings.builder()
            .put("foo.test.bar", 7)
            .put("foo.test.name", "invalid")
            .build(),
            false);
    }

    public void testDependentSettingsWithFallback() {
        Setting.AffixSetting<String> nameFallbackSetting =
                Setting.affixKeySetting("fallback.", "name", k -> Setting.simpleString(k, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<String> nameSetting = Setting.affixKeySetting(
                "foo.",
                "name",
                k -> Setting.simpleString(
                        k,
                        "_na_".equals(k)
                                ? nameFallbackSetting.getConcreteSettingForNamespace(k)
                                : nameFallbackSetting.getConcreteSetting(k.replaceAll("^foo", "fallback")),
                        Property.Dynamic,
                        Property.NodeScope));
        Setting.AffixSetting<Integer> barSetting = Setting.affixKeySetting(
            "foo.",
            "bar",
            k -> Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope),
            () -> nameSetting);

        final AbstractScopedSettings service =
                new ClusterSettings(Settings.EMPTY,new HashSet<>(List.of(nameFallbackSetting, nameSetting, barSetting)));

        assertThatThrownBy(() -> service.validate(Settings.builder().put("foo.test.bar", 7).build(), true))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("missing required setting [foo.test.name] for setting [foo.test.bar]");

        service.validate(Settings.builder().put("foo.test.name", "test").put("foo.test.bar", 7).build(), true);
        service.validate(Settings.builder().put("fallback.test.name", "test").put("foo.test.bar", 7).build(), true);
    }

    @Test
    public void testAddConsumerAffix() {
        Setting.AffixSetting<Integer> intSetting = Setting.affixKeySetting("foo.", "bar",
            k ->  Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<List<Integer>> listSetting = Setting.affixKeySetting("foo.", "list",
            k -> Setting.listSetting(k, List.of("1"), Integer::parseInt, new ArrayType<>(DataTypes.INTEGER), Property.Dynamic, Property.NodeScope));
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY,new HashSet<>(List.of(intSetting, listSetting)));
        Map<String, List<Integer>> listResults = new HashMap<>();
        Map<String, Integer> intResults = new HashMap<>();

        BiConsumer<String, Integer> intConsumer = intResults::put;
        BiConsumer<String, List<Integer>> listConsumer = listResults::put;

        service.addAffixUpdateConsumer(listSetting, listConsumer, (s, k) -> {});
        service.addAffixUpdateConsumer(intSetting, intConsumer, (s, k) -> {});
        assertThat(listResults.size()).isEqualTo(0);
        assertThat(intResults.size()).isEqualTo(0);
        service.applySettings(Settings.builder()
            .put("foo.test.bar", 2)
            .put("foo.test_1.bar", 7)
            .putList("foo.test_list.list", "16", "17")
            .putList("foo.test_list_1.list", "18", "19", "20")
            .build());
        assertThat(intResults.get("test").intValue()).isEqualTo(2);
        assertThat(intResults.get("test_1").intValue()).isEqualTo(7);
        assertThat(listResults.get("test_list")).isEqualTo(List.of(16, 17));
        assertThat(listResults.get("test_list_1")).isEqualTo(List.of(18, 19, 20));
        assertThat(listResults.size()).isEqualTo(2);
        assertThat(intResults.size()).isEqualTo(2);

        listResults.clear();
        intResults.clear();

        service.applySettings(Settings.builder()
            .put("foo.test.bar", 2)
            .put("foo.test_1.bar", 8)
            .putList("foo.test_list.list", "16", "17")
            .putNull("foo.test_list_1.list")
            .build());
        assertThat(intResults.get("test")).as("test wasn't changed").isNull();
        assertThat(intResults.get("test_1").intValue()).isEqualTo(8);
        assertThat(listResults.get("test_list")).as("test_list wasn't changed").isNull();
        assertThat(listResults.get("test_list_1")).isEqualTo(List.of(1)); // reset to default
        assertThat(listResults.size()).isEqualTo(1);
        assertThat(intResults.size()).isEqualTo(1);
    }

    @Test
    public void testAddConsumerAffixMap() {
        Setting.AffixSetting<Integer> intSetting = Setting.affixKeySetting("foo.", "bar",
            k ->  Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<List<Integer>> listSetting = Setting.affixKeySetting("foo.", "list",
            k -> Setting.listSetting(k, List.of("1"), Integer::parseInt, DataTypes.INTEGER_ARRAY, Property.Dynamic, Property.NodeScope));
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY,new HashSet<>(List.of(intSetting, listSetting)));
        Map<String, List<Integer>> listResults = new HashMap<>();
        Map<String, Integer> intResults = new HashMap<>();

        Consumer<Map<String,Integer>> intConsumer = map -> {
            intResults.clear();
            intResults.putAll(map);
        };
        Consumer<Map<String, List<Integer>>> listConsumer = map -> {
            listResults.clear();
            listResults.putAll(map);
        };
        service.addAffixMapUpdateConsumer(listSetting, listConsumer, (s, k) -> {});
        service.addAffixMapUpdateConsumer(intSetting, intConsumer, (s, k) -> {});
        assertThat(listResults.size()).isEqualTo(0);
        assertThat(intResults.size()).isEqualTo(0);
        service.applySettings(Settings.builder()
            .put("foo.test.bar", 2)
            .put("foo.test_1.bar", 7)
            .putList("foo.test_list.list", "16", "17")
            .putList("foo.test_list_1.list", "18", "19", "20")
            .build());
        assertThat(intResults.get("test").intValue()).isEqualTo(2);
        assertThat(intResults.get("test_1").intValue()).isEqualTo(7);
        assertThat(listResults.get("test_list")).isEqualTo(List.of(16, 17));
        assertThat(listResults.get("test_list_1")).isEqualTo(List.of(18, 19, 20));
        assertThat(listResults.size()).isEqualTo(2);
        assertThat(intResults.size()).isEqualTo(2);

        service.applySettings(Settings.builder()
            .put("foo.test.bar", 2)
            .put("foo.test_1.bar", 7)
            .putList("foo.test_list.list", "16", "17")
            .putList("foo.test_list_1.list", "18", "19", "20")
            .build());

        assertThat(intResults.get("test").intValue()).isEqualTo(2);
        assertThat(intResults.get("test_1").intValue()).isEqualTo(7);
        assertThat(listResults.get("test_list")).isEqualTo(List.of(16, 17));
        assertThat(listResults.get("test_list_1")).isEqualTo(List.of(18, 19, 20));
        assertThat(listResults.size()).isEqualTo(2);
        assertThat(intResults.size()).isEqualTo(2);

        listResults.clear();
        intResults.clear();

        service.applySettings(Settings.builder()
            .put("foo.test.bar", 2)
            .put("foo.test_1.bar", 8)
            .putList("foo.test_list.list", "16", "17")
            .putNull("foo.test_list_1.list")
            .build());
        assertThat(intResults.get("test")).as("test wasn't changed").isNull();
        assertThat(intResults.get("test_1").intValue()).isEqualTo(8);
        assertThat(listResults.get("test_list")).as("test_list wasn't changed").isNull();
        assertThat(listResults.get("test_list_1")).isEqualTo(List.of(1)); // reset to default
        assertThat(listResults.size()).isEqualTo(1);
        assertThat(intResults.size()).isEqualTo(1);
    }

    @Test
    public void testAffixMapConsumerNotCalledWithNull() {
        Setting.AffixSetting<Integer> prefixSetting = Setting.prefixKeySetting("eggplant.",
                k ->  Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<Integer> otherSetting = Setting.prefixKeySetting("other.",
                k ->  Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope));
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY,new HashSet<>(List.of(prefixSetting, otherSetting)));
        Map<String, Integer> affixResults = new HashMap<>();

        Consumer<Map<String,Integer>> consumer = map -> {
            logger.info("--> consuming settings {}", map);
            affixResults.clear();
            affixResults.putAll(map);
        };
        service.addAffixMapUpdateConsumer(prefixSetting, consumer, (s, k) -> {});
        assertThat(affixResults.size()).isEqualTo(0);
        service.applySettings(Settings.builder()
                .put("eggplant._name", 2)
                .build());
        assertThat(affixResults).hasSize(1);
        assertThat(affixResults.get("_name")).isEqualTo(2);

        service.applySettings(Settings.builder()
                .put("eggplant._name", 2)
                .put("other.thing", 3)
                .build());

        assertThat(affixResults.get("_name")).isEqualTo(2);
    }


    @Test
    public void testApply() {
        Setting<Integer> testSetting = Setting.intSetting("foo.bar", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> testSetting2 = Setting.intSetting("foo.bar.baz", 1, Property.Dynamic, Property.NodeScope);
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY, new HashSet<>(List.of(testSetting, testSetting2)));

        AtomicInteger consumer = new AtomicInteger();
        service.addSettingsUpdateConsumer(testSetting, consumer::set);
        AtomicInteger consumer2 = new AtomicInteger();
        service.addSettingsUpdateConsumer(testSetting2, consumer2::set, s -> assertThat(s).isGreaterThan(0));

        AtomicInteger aC = new AtomicInteger();
        AtomicInteger bC = new AtomicInteger();
        service.addSettingsUpdateConsumer(testSetting, testSetting2, (a, b) -> {
            aC.set(a);
            bC.set(b);
        });

        assertThat(consumer.get()).isEqualTo(0);
        assertThat(consumer2.get()).isEqualTo(0);
        assertThat(aC.get()).isEqualTo(0);
        assertThat(bC.get()).isEqualTo(0);
        try {
            service.applySettings(Settings.builder().put("foo.bar", 2).put("foo.bar.baz", -15).build());
            fail("invalid value");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage()).isEqualTo("illegal value can't update [foo.bar.baz] from [1] to [-15]");
        }
        assertThat(consumer.get()).isEqualTo(0);
        assertThat(consumer2.get()).isEqualTo(0);
        assertThat(aC.get()).isEqualTo(0);
        assertThat(bC.get()).isEqualTo(0);
        assertThatThrownBy(() ->
            service.validateUpdate(Settings.builder().put("foo.bar", 2).put("foo.bar.baz", -15).build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("illegal value can't update [foo.bar.baz] from [1] to [-15]");

        assertThatThrownBy(() ->
            service.validateUpdate(Settings.builder()
                .put(INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
                .put(SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
                .build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("[cluster.routing.allocation.balance.index] and [cluster.routing.allocation.balance.shard] " +
                        "cannot both be set to 0.0");

        AbstractScopedSettings serviceBalanceFactor1 = new ClusterSettings(Settings.builder()
                .put(INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.0f).build(), Set.of());
        assertThatThrownBy(() ->
            serviceBalanceFactor1.validateUpdate(Settings.builder()
                .put(SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
                .build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("[cluster.routing.allocation.balance.index] and [cluster.routing.allocation.balance.shard] " +
                "cannot both be set to 0.0");
        AbstractScopedSettings serviceBalanceFactor2 = new ClusterSettings(Settings.builder()
            .put(SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.0f).build(), Set.of());
        assertThatThrownBy(() ->
            serviceBalanceFactor2.validateUpdate(Settings.builder()
                .put(INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
                .build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("[cluster.routing.allocation.balance.index] and [cluster.routing.allocation.balance.shard] " +
                "cannot both be set to 0.0");

        assertThat(consumer.get()).isEqualTo(0);
        assertThat(consumer2.get()).isEqualTo(0);
        assertThat(aC.get()).isEqualTo(0);
        assertThat(bC.get()).isEqualTo(0);
        service.validateUpdate(Settings.builder().put("foo.bar", 2).put("foo.bar.baz", 15).build());
        assertThat(consumer.get()).isEqualTo(0);
        assertThat(consumer2.get()).isEqualTo(0);
        assertThat(aC.get()).isEqualTo(0);
        assertThat(bC.get()).isEqualTo(0);

        service.applySettings(Settings.builder().put("foo.bar", 2).put("foo.bar.baz", 15).build());
        assertThat(consumer.get()).isEqualTo(2);
        assertThat(consumer2.get()).isEqualTo(15);
        assertThat(aC.get()).isEqualTo(2);
        assertThat(bC.get()).isEqualTo(15);
    }

    @Test
    public void testGet() {
        ClusterSettings settings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        // affix setting - complex matcher
        Setting<?> setting = settings.get("cluster.routing.allocation.require.value");
        assertThat(FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSetting("cluster.routing.allocation.require.value")).isEqualTo(setting);

        setting = settings.get("cluster.routing.allocation.total_shards_per_node");
        assertThat(setting).isEqualTo(ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING);
    }

    @Test
    public void testIsDynamic(){
        ClusterSettings settings =
            new ClusterSettings(Settings.EMPTY,
                new HashSet<>(List.of(Setting.intSetting("foo.bar", 1, Property.Dynamic, Property.NodeScope),
                    Setting.intSetting("foo.bar.baz", 1, Property.NodeScope))));
        assertThat(settings.isDynamicSetting("foo.bar.baz")).isFalse();
        assertThat(settings.isDynamicSetting("foo.bar")).isTrue();
        assertThat(settings.get("foo.bar.baz")).isNotNull();
    }

    @Test
    public void testIsFinal() {
        ClusterSettings settings =
            new ClusterSettings(Settings.EMPTY,
                new HashSet<>(List.of(Setting.intSetting("foo.int", 1, Property.Final, Property.NodeScope),
                    Setting.groupSetting("foo.group.",  Property.Final, Property.NodeScope),
                    Setting.groupSetting("foo.list.",  Property.Final, Property.NodeScope),
                    Setting.intSetting("foo.int.baz", 1, Property.NodeScope))));

        assertThat(settings.isFinalSetting("foo.int.baz")).isFalse();
        assertThat(settings.isFinalSetting("foo.int")).isTrue();

        assertThat(settings.isFinalSetting("foo.list")).isFalse();
        assertThat(settings.isFinalSetting("foo.list.0.key")).isTrue();
        assertThat(settings.isFinalSetting("foo.list.key")).isTrue();

        assertThat(settings.isFinalSetting("foo.group")).isFalse();
        assertThat(settings.isFinalSetting("foo.group.key")).isTrue();
    }

    @Test
    public void testDiff() throws IOException {
        Setting<Integer> fooBarBaz = Setting.intSetting("foo.bar.baz", 1, Property.NodeScope);
        Setting<Integer> fooBar = Setting.intSetting("foo.bar", 1, Property.Dynamic, Property.NodeScope);
        Setting<Settings> someGroup = Setting.groupSetting("some.group.", Property.Dynamic, Property.NodeScope);
        Setting<Boolean> someAffix = Setting.affixKeySetting("some.prefix.", "somekey", key -> Setting.boolSetting(key, true,
            Property.NodeScope));
        Setting<List<String>> foorBarQuux =
                Setting.listSetting("foo.bar.quux", List.of("a", "b", "c"), Function.identity(), DataTypes.STRING, Property.NodeScope);
        ClusterSettings settings = new ClusterSettings(Settings.EMPTY, new HashSet<>(List.of(fooBar, fooBarBaz, foorBarQuux,
            someGroup, someAffix)));
        Settings diff = settings.diff(Settings.builder().put("foo.bar", 5).build(), Settings.EMPTY);
        assertThat(diff.size()).isEqualTo(2);
        assertThat(diff.getAsInt("foo.bar.baz", null)).isEqualTo(1);
        assertThat(List.of("a", "b", "c")).isEqualTo(diff.getAsList("foo.bar.quux", null));

        diff = settings.diff(
                Settings.builder().put("foo.bar", 5).build(),
                Settings.builder().put("foo.bar.baz", 17).putList("foo.bar.quux", "d", "e", "f").build());
        assertThat(diff.size()).isEqualTo(2);
        assertThat(diff.getAsInt("foo.bar.baz", null)).isEqualTo(17);
        assertThat(List.of("d", "e", "f")).isEqualTo(diff.getAsList("foo.bar.quux", null));

        diff = settings.diff(
            Settings.builder().put("some.group.foo", 5).build(),
            Settings.builder().put("some.group.foobar", 17).put("some.group.foo", 25).build());
        assertThat(diff.size()).isEqualTo(4);
        assertThat(diff.getAsInt("some.group.foobar", null)).isEqualTo(17);
        assertThat(diff.get("some.group.foo")).isNull();
        assertThat(List.of("a", "b", "c")).isEqualTo(diff.getAsList("foo.bar.quux", null));
        assertThat(diff.getAsInt("foo.bar.baz", null)).isEqualTo(1);
        assertThat(diff.getAsInt("foo.bar", null)).isEqualTo(1);

        diff = settings.diff(
            Settings.builder().put("some.prefix.foo.somekey", 5).build(),
            Settings.builder().put("some.prefix.foobar.somekey", 17).put("some.prefix.foo.somekey", 18).build());
        assertThat(diff.size()).isEqualTo(4);
        assertThat(diff.getAsInt("some.prefix.foobar.somekey", null)).isEqualTo(17);
        assertThat(diff.get("some.prefix.foo.somekey")).isNull();
        assertThat(List.of("a", "b", "c")).isEqualTo(diff.getAsList("foo.bar.quux", null));
        assertThat(diff.getAsInt("foo.bar.baz", null)).isEqualTo(1);
        assertThat(diff.getAsInt("foo.bar", null)).isEqualTo(1);
    }

    @Test
    public void testDiffWithAffixAndComplexMatcher() {
        Setting<Integer> fooBarBaz = Setting.intSetting("foo.bar.baz", 1, Property.NodeScope);
        Setting<Integer> fooBar = Setting.intSetting("foo.bar", 1, Property.Dynamic, Property.NodeScope);
        Setting<Settings> someGroup = Setting.groupSetting("some.group.", Property.Dynamic, Property.NodeScope);
        Setting<Boolean> someAffix = Setting.affixKeySetting("some.prefix.", "somekey", key -> Setting.boolSetting(key, true,
            Property.NodeScope));
        Setting<List<String>> foorBarQuux = Setting.affixKeySetting("foo.", "quux",
            key -> Setting.listSetting(key,  List.of("a", "b", "c"), Function.identity(), DataTypes.STRING_ARRAY, Property.NodeScope));
        ClusterSettings settings = new ClusterSettings(Settings.EMPTY, new HashSet<>(List.of(fooBar, fooBarBaz, foorBarQuux,
            someGroup, someAffix)));
        Settings diff = settings.diff(Settings.builder().put("foo.bar", 5).build(), Settings.EMPTY);
        assertThat(diff.size()).isEqualTo(1);
        assertThat(diff.getAsInt("foo.bar.baz", null)).isEqualTo(1);
        assertThat(diff.getAsList("foo.bar.quux", null)).isNull(); // affix settings don't know their concrete keys

        diff = settings.diff(
            Settings.builder().put("foo.bar", 5).build(),
            Settings.builder().put("foo.bar.baz", 17).putList("foo.bar.quux", "d", "e", "f").build());
        assertThat(diff.size()).isEqualTo(2);
        assertThat(diff.getAsInt("foo.bar.baz", null)).isEqualTo(17);
        assertThat(List.of("d", "e", "f")).isEqualTo(diff.getAsList("foo.bar.quux", null));

        diff = settings.diff(
            Settings.builder().put("some.group.foo", 5).build(),
            Settings.builder().put("some.group.foobar", 17).put("some.group.foo", 25).build());
        assertThat(diff.size()).isEqualTo(3);
        assertThat(diff.getAsInt("some.group.foobar", null)).isEqualTo(17);
        assertThat(diff.get("some.group.foo")).isNull();
        assertThat(diff.getAsList("foo.bar.quux", null)).isNull(); // affix settings don't know their concrete keys
        assertThat(diff.getAsInt("foo.bar.baz", null)).isEqualTo(1);
        assertThat(diff.getAsInt("foo.bar", null)).isEqualTo(1);

        diff = settings.diff(
            Settings.builder().put("some.prefix.foo.somekey", 5).build(),
            Settings.builder().put("some.prefix.foobar.somekey", 17).put("some.prefix.foo.somekey", 18).build());
        assertThat(diff.size()).isEqualTo(3);
        assertThat(diff.getAsInt("some.prefix.foobar.somekey", null)).isEqualTo(17);
        assertThat(diff.get("some.prefix.foo.somekey")).isNull();
        assertThat(diff.getAsList("foo.bar.quux", null)).isNull(); // affix settings don't know their concrete keys
        assertThat(diff.getAsInt("foo.bar.baz", null)).isEqualTo(1);
        assertThat(diff.getAsInt("foo.bar", null)).isEqualTo(1);

        diff = settings.diff(
            Settings.builder().put("some.prefix.foo.somekey", 5).build(),
            Settings.builder().put("some.prefix.foobar.somekey", 17).put("some.prefix.foo.somekey", 18)
            .putList("foo.bar.quux", "x", "y", "z")
            .putList("foo.baz.quux", "d", "e", "f")
                .build());
        assertThat(diff.size()).isEqualTo(5);
        assertThat(diff.getAsInt("some.prefix.foobar.somekey", null)).isEqualTo(17);
        assertThat(diff.get("some.prefix.foo.somekey")).isNull();
        assertThat(List.of("x", "y", "z")).isEqualTo(diff.getAsList("foo.bar.quux", null));
        assertThat(List.of("d", "e", "f")).isEqualTo(diff.getAsList("foo.baz.quux", null));
        assertThat(diff.getAsInt("foo.bar.baz", null)).isEqualTo(1);
        assertThat(diff.getAsInt("foo.bar", null)).isEqualTo(1);
    }

    @Test
    public void testGetSetting() {
        IndexScopedSettings settings = new IndexScopedSettings(
           Settings.EMPTY,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        IndexScopedSettings copy = settings.copy(Settings.builder().put("index.store.type", "boom").build(),
                newIndexMeta("foo", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 3).build()));
        assertThat(copy.get(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING).intValue()).isEqualTo(3);
        assertThat(copy.get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING).intValue()).isEqualTo(1);
        assertThat(copy.get(IndexModule.INDEX_STORE_TYPE_SETTING)).isEqualTo("boom"); // test fallback to node settings
    }

    @Test
    public void testValidateWithSuggestion() {
        IndexScopedSettings settings = new IndexScopedSettings(
            Settings.EMPTY,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        assertThatThrownBy(() -> settings.validate(Settings.builder().put("index.numbe_of_replica", "1").build(), false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("unknown setting [index.numbe_of_replica] did you mean [index.number_of_replicas]?");
    }

    @Test
    public void testValidate() {
        IndexScopedSettings settings = new IndexScopedSettings(
            Settings.EMPTY,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        String unknownMsgSuffix = " please check that any required plugins are installed, or check the breaking changes documentation for" +
            " removed settings";
        settings.validate(Settings.builder().put("index.store.type", "boom").build(), false);

        assertThatThrownBy(() ->
            settings.validate(Settings.builder().put("index.store.type", "boom").put("i.am.not.a.setting", true).build(), false)
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("unknown setting [i.am.not.a.setting]" + unknownMsgSuffix);

        assertThatThrownBy(() ->
            settings.validate(Settings.builder().put("index.store.type", "boom").put("index.number_of_replicas", true).build(), false)
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Failed to parse value [true] for setting [index.number_of_replicas]");

        assertThatThrownBy(() ->
            settings.validate("index.number_of_replicas", Settings.builder().put("index.number_of_replicas", "true").build(), false)
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Failed to parse value [true] for setting [index.number_of_replicas]");
    }

    public static IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        Settings build = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(indexSettings)
            .build();
        return IndexMetadata.builder(name).settings(build).build();
    }

    @Test
    public void testKeyPattern() {
        assertThat(AbstractScopedSettings.isValidKey("a.b.c-b.d")).isTrue();
        assertThat(AbstractScopedSettings.isValidKey("a.b.c.d")).isTrue();
        assertThat(AbstractScopedSettings.isValidKey("a.b_012.c_b.d")).isTrue();
        assertThat(AbstractScopedSettings.isValidKey("a")).isTrue();
        assertThat(AbstractScopedSettings.isValidKey("a b")).isFalse();
        assertThat(AbstractScopedSettings.isValidKey("")).isFalse();
        assertThat(AbstractScopedSettings.isValidKey("\"")).isFalse();

        try {
            new IndexScopedSettings(
                Settings.EMPTY, Collections.singleton(Setting.groupSetting("foo.bar.", Property.IndexScope)));
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("illegal settings key: [foo.bar.] must start with [index.]");
        }

        try {
            new IndexScopedSettings(
                Settings.EMPTY, Collections.singleton(Setting.simpleString("foo.bar", Property.IndexScope)));
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("illegal settings key: [foo.bar] must start with [index.]");
        }

        try {
            new IndexScopedSettings(
                Settings.EMPTY, Collections.singleton(Setting.groupSetting("index. foo.", Property.IndexScope)));
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("illegal settings key: [index. foo.]");
        }
        new IndexScopedSettings(
            Settings.EMPTY, Collections.singleton(Setting.groupSetting("index.", Property.IndexScope)));
        try {
            new IndexScopedSettings(
                Settings.EMPTY, Collections.singleton(Setting.boolSetting("index.", true, Property.IndexScope)));
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("illegal settings key: [index.]");
        }
        new IndexScopedSettings(
            Settings.EMPTY, Collections.singleton(Setting.boolSetting("index.boo", true, Property.IndexScope)));

        new ClusterSettings(
            Settings.EMPTY, Collections.singleton(Setting.boolSetting("index.boo", true, Property.NodeScope)));
    }

    @Test
    public void testAffixKeyPattern() {
        assertThat(AbstractScopedSettings.isValidAffixKey("prefix.*.suffix")).isTrue();
        assertThat(AbstractScopedSettings.isValidAffixKey("prefix.*.split.suffix")).isTrue();
        assertThat(AbstractScopedSettings.isValidAffixKey("split.prefix.*.split.suffix")).isTrue();
        assertThat(AbstractScopedSettings.isValidAffixKey("prefix.*.suffix.")).isFalse();
        assertThat(AbstractScopedSettings.isValidAffixKey("prefix.*")).isFalse();
        assertThat(AbstractScopedSettings.isValidAffixKey("*.suffix")).isFalse();
        assertThat(AbstractScopedSettings.isValidAffixKey("*")).isFalse();
        assertThat(AbstractScopedSettings.isValidAffixKey("")).isFalse();
    }

    @Test
    public void testLoggingUpdates() {
        final Level level = LogManager.getRootLogger().getLevel();
        final Level testLevel = LogManager.getLogger("test").getLevel();
        Level property = randomFrom(Level.values());
        Settings.Builder builder = Settings.builder().put("logger.level", property);
        try {
            ClusterSettings settings = new ClusterSettings(builder.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            assertThatThrownBy(() -> settings.validate(Settings.builder().put("logger._root", "boom").build(), false))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown level constant [BOOM].");
            assertThat(LogManager.getRootLogger().getLevel()).isEqualTo(level);
            settings.applySettings(Settings.builder().put("logger._root", "TRACE").build());
            assertThat(LogManager.getRootLogger().getLevel()).isEqualTo(Level.TRACE);
            settings.applySettings(Settings.builder().build());
            assertThat(LogManager.getRootLogger().getLevel()).isEqualTo(property);
            settings.applySettings(Settings.builder().put("logger.test", "TRACE").build());
            assertThat(LogManager.getLogger("test").getLevel()).isEqualTo(Level.TRACE);
            settings.applySettings(Settings.builder().build());
            assertThat(LogManager.getLogger("test").getLevel()).isEqualTo(property);
        } finally {
            Loggers.setLevel(LogManager.getRootLogger(), level);
            Loggers.setLevel(LogManager.getLogger("test"), testLevel);
        }
    }

    @Test
    public void testFallbackToLoggerLevel() {
        final Level level = LogManager.getRootLogger().getLevel();
        try {
            ClusterSettings settings =
                new ClusterSettings(Settings.builder().put("logger.level", "ERROR").build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            assertThat(LogManager.getRootLogger().getLevel()).isEqualTo(level);
            settings.applySettings(Settings.builder().put("logger._root", "TRACE").build());
            assertThat(LogManager.getRootLogger().getLevel()).isEqualTo(Level.TRACE);
            settings.applySettings(Settings.builder().build()); // here we fall back to 'logger.level' which is our default.
            assertThat(LogManager.getRootLogger().getLevel()).isEqualTo(Level.ERROR);
        } finally {
            Loggers.setLevel(LogManager.getRootLogger(), level);
        }
    }

    @Test
    public void testOverlappingComplexMatchSettings() {
        Set<Setting<?>> settings = new LinkedHashSet<>(2);
        final boolean groupFirst = randomBoolean();
        final Setting<?> groupSetting = Setting.groupSetting("foo.", Property.NodeScope);
        final Setting<?> listSetting =
            Setting.listSetting("foo.bar", Collections.emptyList(), Function.identity(), DataTypes.STRING_ARRAY, Property.NodeScope);
        settings.add(groupFirst ? groupSetting : listSetting);
        settings.add(groupFirst ? listSetting : groupSetting);

        try {
            new ClusterSettings(Settings.EMPTY, settings);
            fail("an exception should have been thrown because settings overlap");
        } catch (IllegalArgumentException e) {
            if (groupFirst) {
                assertThat(e.getMessage()).isEqualTo("complex setting key: [foo.bar] overlaps existing setting key: [foo.]");
            } else {
                assertThat(e.getMessage()).isEqualTo("complex setting key: [foo.] overlaps existing setting key: [foo.bar]");
            }
        }
    }

    @Test
    public void testUpdateNumberOfShardsFail() {
        IndexScopedSettings settings = new IndexScopedSettings(Settings.EMPTY,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        assertThatThrownBy(() -> settings.updateSettings(Settings.builder().put("index.number_of_shards", 8).build(),
                Settings.builder(), Settings.builder(), "index")
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("final index setting [index.number_of_shards], not updateable");
    }

    @Test
    public void testFinalSettingUpdateFail() {
        Setting<Integer> finalSetting = Setting.intSetting("some.final.setting", 1, Property.Final, Property.NodeScope);
        Setting<Settings> finalGroupSetting = Setting.groupSetting("some.final.group.", Property.Final, Property.NodeScope);
        Settings currentSettings = Settings.builder()
            .put("some.final.setting", 9)
            .put("some.final.group.foo", 7)
            .build();
        ClusterSettings service = new ClusterSettings(currentSettings
            , new HashSet<>(List.of(finalSetting, finalGroupSetting)));

        assertThatThrownBy(() ->
            service.updateDynamicSettings(Settings.builder().put("some.final.setting", 8).build(),
                Settings.builder().put(currentSettings), Settings.builder(), "node")
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("final node setting [some.final.setting]");

        assertThatThrownBy(() ->
            service.updateDynamicSettings(Settings.builder().putNull("some.final.setting").build(),
                Settings.builder().put(currentSettings), Settings.builder(), "node")
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("final node setting [some.final.setting]");

        assertThatThrownBy(() ->
            service.updateSettings(Settings.builder().put("some.final.group.new", 8).build(),
                Settings.builder().put(currentSettings), Settings.builder(), "node")
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("final node setting [some.final.group.new]");

        assertThatThrownBy(() ->
            service.updateSettings(Settings.builder().put("some.final.group.foo", 5).build(),
                Settings.builder().put(currentSettings), Settings.builder(), "node")
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("final node setting [some.final.group.foo]");
    }

    @Test
    public void testInternalIndexSettingsFailsValidation() {
        final Setting<String> indexInternalSetting = Setting.simpleString("index.internal", Property.InternalIndex, Property.IndexScope);
        final IndexScopedSettings indexScopedSettings =
                new IndexScopedSettings(Settings.EMPTY, Collections.singleton(indexInternalSetting));
        assertThatThrownBy(() -> {
            final Settings settings = Settings.builder().put("index.internal", "internal").build();
            indexScopedSettings.validate(settings, false, /* validateInternalOrPrivateIndex */ true);
        }).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("can not update internal setting [index.internal]; this setting is managed via a dedicated API");
    }

    @Test
    public void testPrivateIndexSettingsFailsValidation() {
        final Setting<String> indexInternalSetting = Setting.simpleString("index.private", Property.PrivateIndex, Property.IndexScope);
        final IndexScopedSettings indexScopedSettings =
                new IndexScopedSettings(Settings.EMPTY, Collections.singleton(indexInternalSetting));
        assertThatThrownBy(() -> {
            final Settings settings = Settings.builder().put("index.private", "private").build();
            indexScopedSettings.validate(settings, false, /* validateInternalOrPrivateIndex */ true);
        }).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("can not update private setting [index.private]; this setting is managed by CrateDB");
    }

    @Test
    public void testInternalIndexSettingsSkipValidation() {
        final Setting<String> internalIndexSetting = Setting.simpleString("index.internal", Property.InternalIndex, Property.IndexScope);
        final IndexScopedSettings indexScopedSettings =
                new IndexScopedSettings(Settings.EMPTY, Collections.singleton(internalIndexSetting));
        // nothing should happen, validation should not throw an exception
        final Settings settings = Settings.builder().put("index.internal", "internal").build();
        indexScopedSettings.validate(settings, false, /* validateInternalOrPrivateIndex */ false);
    }

    @Test
    public void testPrivateIndexSettingsSkipValidation() {
        final Setting<String> internalIndexSetting = Setting.simpleString("index.private", Property.PrivateIndex, Property.IndexScope);
        final IndexScopedSettings indexScopedSettings =
                new IndexScopedSettings(Settings.EMPTY, Collections.singleton(internalIndexSetting));
        // nothing should happen, validation should not throw an exception
        final Settings settings = Settings.builder().put("index.private", "private").build();
        indexScopedSettings.validate(settings, false, /* validateInternalOrPrivateIndex */ false);
    }

    @Test
    public void testUpgradeSetting() {
        final Setting<String> oldSetting = Setting.simpleString("foo.old", Property.NodeScope);
        final Setting<String> newSetting = Setting.simpleString("foo.new", Property.NodeScope);
        final Setting<String> remainingSetting = Setting.simpleString("foo.remaining", Property.NodeScope);

        final AbstractScopedSettings service =
                new ClusterSettings(
                        Settings.EMPTY,
                        List.of(),
                        new HashSet<>(List.of(oldSetting, newSetting, remainingSetting)),
                        Collections.singleton(new SettingUpgrader<String>() {

                            @Override
                            public Setting<String> getSetting() {
                                return oldSetting;
                            }

                            @Override
                            public String getKey(final String key) {
                                return "foo.new";
                            }

                            @Override
                            public String getValue(final String value) {
                                return "new." + value;
                            }

                        }));

        final Settings settings =
                Settings.builder()
                        .put("foo.old", randomAlphaOfLength(8))
                        .put("foo.remaining", randomAlphaOfLength(8))
                        .build();
        final Settings upgradedSettings = service.upgradeSettings(settings);
        assertThat(oldSetting.exists(upgradedSettings)).isFalse();
        assertThat(newSetting.exists(upgradedSettings)).isTrue();
        assertThat(newSetting.get(upgradedSettings)).isEqualTo("new." + oldSetting.get(settings));
        assertThat(remainingSetting.exists(upgradedSettings)).isTrue();
        assertThat(remainingSetting.get(upgradedSettings)).isEqualTo(remainingSetting.get(settings));
    }

    @Test
    public void testUpgradeSettingsNoChangesPreservesInstance() {
        final Setting<String> oldSetting = Setting.simpleString("foo.old", Property.NodeScope);
        final Setting<String> newSetting = Setting.simpleString("foo.new", Property.NodeScope);
        final Setting<String> remainingSetting = Setting.simpleString("foo.remaining", Property.NodeScope);

        final AbstractScopedSettings service =
                new ClusterSettings(
                        Settings.EMPTY,
                        List.of(),
                        new HashSet<>(List.of(oldSetting, newSetting, remainingSetting)),
                        Collections.singleton(new SettingUpgrader<String>() {

                            @Override
                            public Setting<String> getSetting() {
                                return oldSetting;
                            }

                            @Override
                            public String getKey(final String key) {
                                return "foo.new";
                            }

                        }));

        final Settings settings = Settings.builder().put("foo.remaining", randomAlphaOfLength(8)).build();
        final Settings upgradedSettings = service.upgradeSettings(settings);
        assertThat(upgradedSettings).isSameAs(settings);
    }

    @Test
    public void testUpgradeComplexSetting() {
        final Setting.AffixSetting<String> oldSetting =
                Setting.affixKeySetting("foo.old.", "suffix", key -> Setting.simpleString(key, Property.NodeScope));
        final Setting.AffixSetting<String> newSetting =
                Setting.affixKeySetting("foo.new.", "suffix", key -> Setting.simpleString(key, Property.NodeScope));
        final Setting.AffixSetting<String> remainingSetting =
                Setting.affixKeySetting("foo.remaining.", "suffix", key -> Setting.simpleString(key, Property.NodeScope));

        final AbstractScopedSettings service =
                new ClusterSettings(
                        Settings.EMPTY,
                        List.of(),
                        new HashSet<>(List.of(oldSetting, newSetting, remainingSetting)),
                        Collections.singleton(new SettingUpgrader<String>() {

                            @Override
                            public Setting<String> getSetting() {
                                return oldSetting;
                            }

                            @Override
                            public String getKey(final String key) {
                                return key.replaceFirst("^foo\\.old", "foo\\.new");
                            }

                            @Override
                            public String getValue(final String value) {
                                return "new." + value;
                            }

                        }));

        final int count = randomIntBetween(1, 8);
        final List<String> concretes = new ArrayList<>(count);
        final Settings.Builder builder = Settings.builder();
        for (int i = 0; i < count; i++) {
            final String concrete = randomAlphaOfLength(8);
            concretes.add(concrete);
            builder.put("foo.old." + concrete + ".suffix", randomAlphaOfLength(8));
            builder.put("foo.remaining." + concrete + ".suffix", randomAlphaOfLength(8));
        }
        final Settings settings = builder.build();
        final Settings upgradedSettings = service.upgradeSettings(settings);
        for (final String concrete : concretes) {
            assertThat(oldSetting.getConcreteSettingForNamespace(concrete).exists(upgradedSettings)).isFalse();
            assertThat(newSetting.getConcreteSettingForNamespace(concrete).exists(upgradedSettings)).isTrue();
            assertThat(
                    newSetting.getConcreteSettingForNamespace(concrete).get(upgradedSettings)).isEqualTo("new." + oldSetting.getConcreteSettingForNamespace(concrete).get(settings));
            assertThat(remainingSetting.getConcreteSettingForNamespace(concrete).exists(upgradedSettings)).isTrue();
            assertThat(
                    remainingSetting.getConcreteSettingForNamespace(concrete).get(upgradedSettings)).isEqualTo(remainingSetting.getConcreteSettingForNamespace(concrete).get(settings));
        }
    }

    @Test
    public void testUpgradeListSetting() {
        final Setting<List<String>> oldSetting =
                Setting.listSetting("foo.old", Collections.emptyList(), Function.identity(), DataTypes.STRING_ARRAY, Property.NodeScope);
        final Setting<List<String>> newSetting =
                Setting.listSetting("foo.new", Collections.emptyList(), Function.identity(), DataTypes.STRING_ARRAY, Property.NodeScope);

        final AbstractScopedSettings service =
                new ClusterSettings(
                        Settings.EMPTY,
                        List.of(),
                        new HashSet<>(List.of(oldSetting, newSetting)),
                        Collections.singleton(new SettingUpgrader<List<String>>() {

                            @Override
                            public Setting<List<String>> getSetting() {
                                return oldSetting;
                            }

                            @Override
                            public String getKey(final String key) {
                                return "foo.new";
                            }

                            @Override
                            public List<String> getListValue(final List<String> value) {
                                return value.stream().map(s -> "new." + s).toList();
                            }
                        }));

        final int length = randomIntBetween(0, 16);
        final List<String> values = length == 0 ? Collections.emptyList() : new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            values.add(randomAlphaOfLength(8));
        }

        final Settings settings = Settings.builder().putList("foo.old", values).build();
        final Settings upgradedSettings = service.upgradeSettings(settings);
        assertThat(oldSetting.exists(upgradedSettings)).isFalse();
        assertThat(newSetting.exists(upgradedSettings)).isTrue();
        assertThat(
                newSetting.get(upgradedSettings)).isEqualTo(oldSetting.get(settings).stream().map(s -> "new." + s).toList());
    }

    @Test
    public void test_update_setting_with_dependency() throws Exception {
        Setting<String> low = DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;
        Setting<String> high = DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
        Setting<String> flood = DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING;

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(low, high, flood));
        Settings.Builder target = Settings.builder();
        clusterSettings.updateDynamicSettings(
            Settings.builder().put(high.getKey(), "95%").build(),
            target,
            Settings.builder(),
            "update high to 95%"
        );
        clusterSettings.applySettings(target.build());
        clusterSettings.updateDynamicSettings(
            Settings.builder().put(low.getKey(), "91%").build(),
            Settings.builder(),
            Settings.builder(),
            "update low to 91%"
        );
    }
}

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

package org.elasticsearch.index;


import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.types.DataTypes;

public class IndexSettingsTests extends ESTestCase {

    @Test
    public void testRunListener() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_INDEX_UUID, "0xdeadbeef").build();
        final AtomicInteger integer = new AtomicInteger(0);
        Setting<Integer> integerSetting = Setting.intSetting(
            "index.test.setting.int",
            -1,
            Property.Dynamic,
            Property.IndexScope
        );
        IndexMetadata metaData = newIndexMeta("index", theSettings);
        IndexSettings settings = newIndexSettings(
            newIndexMeta("index", theSettings), Settings.EMPTY, integerSetting);
        settings.getScopedSettings().addSettingsUpdateConsumer(integerSetting, integer::set);

        assertThat(settings.getIndexVersionCreated()).isEqualTo(version);
        assertThat(settings.getUUID()).isEqualTo("0xdeadbeef");

        assertThat(settings.updateIndexMetadata(metaData)).isFalse();
        assertThat(settings.getSettings()).isEqualTo(metaData.getSettings());
        assertThat(integer.get()).isEqualTo(0);
        assertThat(
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(theSettings)
                        .put("index.test.setting.int", 42)
                        .build())))
            .isTrue();
        assertThat(integer.get()).isEqualTo(42);
    }

    @Test
    public void testSettingsUpdateValidator() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_INDEX_UUID, "0xdeadbeef")
            .build();
        final AtomicInteger integer = new AtomicInteger(0);
        Setting<Integer> integerSetting = Setting.intSetting(
            "index.test.setting.int",
            -1,
            Property.Dynamic,
            Property.IndexScope
        );
        IndexMetadata metaData = newIndexMeta("index", theSettings);
        IndexSettings settings = newIndexSettings(
            newIndexMeta("index", theSettings), Settings.EMPTY, integerSetting);
        settings.getScopedSettings().addSettingsUpdateConsumer(
            integerSetting, integer::set,
            (i) -> {
                if (i == 42) {
                    throw new AssertionError("boom");
                }
            });

        assertThat(settings.getIndexVersionCreated()).isEqualTo(version);
        assertThat(settings.getUUID()).isEqualTo("0xdeadbeef");

        assertThat(settings.updateIndexMetadata(metaData)).isFalse();
        assertThat(settings.getSettings()).isEqualTo(metaData.getSettings());
        assertThat(integer.get()).isEqualTo(0);
        assertThatThrownBy(() -> settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(theSettings)
                    .put("index.test.setting.int", 42)
                    .build()))
        ).isExactlyInstanceOf(IllegalArgumentException.class);
        assertThat(
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(theSettings)
                        .put("index.test.setting.int", 41)
                        .build())))
            .isTrue();
        assertThat(integer.get()).isEqualTo(41);
    }

    @Test
    public void testMergedSettingsArePassed() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_INDEX_UUID, "0xdeadbeef")
            .build();
        final AtomicInteger integer = new AtomicInteger(0);
        final StringBuilder builder = new StringBuilder();
        Setting<Integer> integerSetting = Setting.intSetting(
            "index.test.setting.int",
            -1,
            Property.Dynamic,
            Property.IndexScope);
        Setting<String> notUpdated = new Setting<>(
            "index.not.updated",
            "",
            Function.identity(),
            DataTypes.STRING,
            Property.Dynamic,
            Property.IndexScope);

        IndexSettings settings = newIndexSettings(
            newIndexMeta("index", theSettings), Settings.EMPTY, integerSetting, notUpdated);
        settings.getScopedSettings().addSettingsUpdateConsumer(integerSetting, integer::set);
        settings.getScopedSettings().addSettingsUpdateConsumer(notUpdated, builder::append);
        assertThat(integer.get()).isEqualTo(0);
        assertThat(builder.toString()).isEqualTo("");
        IndexMetadata newMetadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(settings.getIndexMetadata().getSettings())
                .put("index.test.setting.int", 42)
                .build()
        );
        assertThat(settings.updateIndexMetadata(newMetadata)).isTrue();
        assertThat(settings.getIndexMetadata()).isSameAs(newMetadata);
        assertThat(integer.get()).isEqualTo(42);
        assertThat(builder.toString()).isEqualTo("");
        integer.set(0);
        assertThat(settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(settings.getIndexMetadata().getSettings())
                .put("index.not.updated", "boom").build())))
            .isTrue();
        assertThat(builder.toString()).isEqualTo("boom");
        assertThat(integer.get())
            .as("not updated - we preserve the old settings")
            .isEqualTo(0);
    }

    @Test
    public void testSettingsConsistency() {
        Version version = VersionUtils.getPreviousVersion();
        IndexMetadata metaData = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                .build()
        );
        final IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertThat(settings.getIndexVersionCreated()).isEqualTo(version);
        assertThat(settings.getUUID()).isEqualTo("_na_");

        assertThatThrownBy(() -> {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put("index.test.setting.int", 42)
                        .build()
                )
            );
        }).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("version mismatch on settings update expected: ");

        // use version number that is unknown
        int unknownVersion = Version.CURRENT.internalId + 200;
        metaData = newIndexMeta(
            "index",
            Settings.builder()
                .put(
                    IndexMetadata.SETTING_VERSION_CREATED, Version.fromId(unknownVersion))
                .build());
        IndexSettings settings2 = new IndexSettings(metaData, Settings.EMPTY);
        assertThat(settings2.getIndexVersionCreated()).isEqualTo(Version.fromId(unknownVersion));
        assertThat(settings2.getUUID()).isEqualTo("_na_");
        settings2.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.fromId(unknownVersion))
                    .put("index.test.setting.int", 42)
                    .build()
            )
        );
        metaData = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "0xdeadbeef")
                .build()
        );
        IndexSettings settings3 = new IndexSettings(metaData, Settings.EMPTY);
        assertThatThrownBy(() -> {
            settings3.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put("index.test.setting.int", 42)
                        .build()
                )
            );
        }).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("uuid mismatch on settings update expected: 0xdeadbeef but was: _na_");
        assertThat(settings3.getSettings()).isEqualTo(metaData.getSettings());
    }

    public IndexSettings newIndexSettings(IndexMetadata metaData, Settings nodeSettings, Setting<?>... settings) {
        Set<Setting<?>> settingSet = new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        if (settings.length > 0) {
            settingSet.addAll(Arrays.asList(settings));
        }
        return new IndexSettings(metaData, nodeSettings, new IndexScopedSettings(Settings.EMPTY, settingSet));
    }

    @Test
    public void testNodeSettingsAreContained() {
        final int numShards = randomIntBetween(1, 10);
        final int numReplicas = randomIntBetween(0, 10);
        Settings theSettings = Settings.builder().
            put("index.foo.bar", 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();

        Settings nodeSettings = Settings.builder().put("index.foo.bar", 43).build();
        final AtomicInteger indexValue = new AtomicInteger(0);
        Setting<Integer> integerSetting = Setting.intSetting(
            "index.foo.bar",
            -1,
            Property.Dynamic,
            Property.IndexScope);
        IndexSettings settings = newIndexSettings(
            newIndexMeta("index", theSettings), nodeSettings, integerSetting);
        settings.getScopedSettings().addSettingsUpdateConsumer(integerSetting, indexValue::set);
        assertThat(settings.getNumberOfReplicas()).isEqualTo(numReplicas);
        assertThat(settings.getNumberOfShards()).isEqualTo(numShards);
        assertThat(indexValue.get()).isEqualTo(0);

        assertTrue(settings.updateIndexMetadata(newIndexMeta("index", Settings.builder().
            put("index.foo.bar", 42)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas + 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build())));

        assertThat(indexValue.get()).isEqualTo(42);
        assertSame(nodeSettings, settings.getNodeSettings());

        assertTrue(settings.updateIndexMetadata(newIndexMeta("index", Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas + 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build())));
        assertThat(indexValue.get()).isEqualTo(43);

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
    public void testUpdateDurability() {
        IndexMetadata metaData = newIndexMeta("index", Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), "async")
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertThat(settings.getTranslogDurability()).isEqualTo(Translog.Durability.ASYNC);
        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), "request")
                    .build()));
        assertThat(settings.getTranslogDurability()).isEqualTo(Translog.Durability.REQUEST);

        metaData = newIndexMeta("index", Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build());
        settings = new IndexSettings(metaData, Settings.EMPTY);
        assertThat(settings.getTranslogDurability()).isEqualTo(Translog.Durability.REQUEST); // test default
    }

    @Test
    public void testRefreshInterval() {
        String refreshInterval = getRandomTimeString();
        IndexMetadata metaData = newIndexMeta("index", Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval)
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertThat(
            TimeValue.parseTimeValue(
                refreshInterval,
                new TimeValue(1, TimeUnit.DAYS),
                IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey())
            ).isEqualTo(settings.getRefreshInterval());
        String newRefreshInterval = getRandomTimeString();
        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), newRefreshInterval)
                    .build()));
        assertThat(
            TimeValue.parseTimeValue(
                newRefreshInterval,
                new TimeValue(1, TimeUnit.DAYS),
                IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey())
            ).isEqualTo(settings.getRefreshInterval());
    }

    private String getRandomTimeString() {
        int refreshIntervalInt = randomFrom(-1, Math.abs(randomInt()));
        String refreshInterval = Integer.toString(refreshIntervalInt);
        if (refreshIntervalInt >= 0) {
            refreshInterval += randomFrom("s", "ms", "h");
        }
        return refreshInterval;
    }

    @Test
    public void testGCDeletesSetting() {
        TimeValue gcDeleteSetting = new TimeValue(Math.abs(randomInt()), TimeUnit.MILLISECONDS);
        IndexMetadata metaData = newIndexMeta("index", Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), gcDeleteSetting.getStringRep())
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertThat(
            TimeValue.parseTimeValue(
                gcDeleteSetting.getStringRep(),
                new TimeValue(1, TimeUnit.DAYS),
                IndexSettings.INDEX_GC_DELETES_SETTING.getKey()).millis())
            .isEqualTo(settings.getGcDeletesInMillis());
        TimeValue newGCDeleteSetting = new TimeValue(Math.abs(randomInt()), TimeUnit.MILLISECONDS);
        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), newGCDeleteSetting.getStringRep())
                    .build()
            )
        );
        assertThat(
            TimeValue.parseTimeValue(
                newGCDeleteSetting.getStringRep(),
                new TimeValue(1, TimeUnit.DAYS),
                IndexSettings.INDEX_GC_DELETES_SETTING.getKey()).millis()
        ).isEqualTo(settings.getGcDeletesInMillis());
        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        IndexSettings.INDEX_GC_DELETES_SETTING.getKey(),
                        (randomBoolean() ? -1 : new TimeValue(-1, TimeUnit.MILLISECONDS)).toString()
                    ).build()
            )
        );
        assertThat(settings.getGcDeletesInMillis()).isEqualTo(-1L);
    }

    @Test
    public void testTranslogFlushSizeThreshold() {
        ByteSizeValue translogFlushThresholdSize = new ByteSizeValue(Math.abs(randomInt()));
        ByteSizeValue actualValue = ByteSizeValue.parseBytesSizeValue(
            translogFlushThresholdSize.getBytes() + "B",
            IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey());
        IndexMetadata metaData = newIndexMeta("index", Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                 translogFlushThresholdSize.getBytes() + "B")
            .build());
        IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertThat(actualValue).isEqualTo(settings.getFlushThresholdSize());
        ByteSizeValue newTranslogFlushThresholdSize = new ByteSizeValue(Math.abs(randomInt()));
        ByteSizeValue actualNewTranslogFlushThresholdSize = ByteSizeValue.parseBytesSizeValue(
            newTranslogFlushThresholdSize.getBytes() + "B",
            IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey());
        settings.updateIndexMetadata(newIndexMeta("index", Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                 newTranslogFlushThresholdSize.getBytes() + "B")
            .build()));
        assertThat(actualNewTranslogFlushThresholdSize).isEqualTo(settings.getFlushThresholdSize());
    }

    @Test
    public void testTranslogGenerationSizeThreshold() {
        final ByteSizeValue size = new ByteSizeValue(Math.abs(randomInt()));
        final String key = IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING.getKey();
        final ByteSizeValue actualValue =
            ByteSizeValue.parseBytesSizeValue(size.getBytes() + "B", key);
        final IndexMetadata metaData =
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(key, size.getBytes() + "B")
                    .build());
        final IndexSettings settings = new IndexSettings(metaData, Settings.EMPTY);
        assertThat(actualValue).isEqualTo(settings.getGenerationThresholdSize());
        final ByteSizeValue newSize = new ByteSizeValue(Math.abs(randomInt()));
        final ByteSizeValue actual = ByteSizeValue.parseBytesSizeValue(newSize.getBytes() + "B", key);
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(key, newSize.getBytes() + "B").build()));
        assertThat(actual).isEqualTo(settings.getGenerationThresholdSize());
    }

    @Test
    public void testPrivateSettingsValidation() {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_CREATION_DATE, System.currentTimeMillis())
            .build();
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(
            settings,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);

        // validation should fail since we are not ignoring private settings
        assertThatThrownBy(() -> indexScopedSettings.validate(settings, randomBoolean()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("unknown setting [index.creation_date]");

            // validation should fail since we are not ignoring private settings
        assertThatThrownBy(() -> indexScopedSettings.validate(settings, randomBoolean(), false, randomBoolean()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("unknown setting [index.creation_date]");

        // nothing should happen since we are ignoring private settings
        indexScopedSettings.validate(settings, randomBoolean(), true, randomBoolean());
    }

    @Test
    public void testArchivedSettingsValidation() {
        final Settings settings = Settings.builder()
            .put(AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX + "foo", System.currentTimeMillis())
            .build();
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(
            settings,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
        );

        // validation should fail since we are not ignoring archived settings
        assertThatThrownBy(() -> indexScopedSettings.validate(settings, randomBoolean()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("unknown setting [archived.foo]");

        // validation should fail since we are not ignoring archived settings
        assertThatThrownBy(() -> indexScopedSettings.validate(settings, randomBoolean(), randomBoolean(), false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("unknown setting [archived.foo]");

        // nothing should happen since we are ignoring archived settings
        indexScopedSettings.validate(settings, randomBoolean(), randomBoolean(), true);
    }

    @Test
    public void testArchiveBrokenIndexSettings() {
        Settings settings =
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(
                Settings.EMPTY,
                e -> {
                    assert false : "should not have been invoked, no unknown settings";
                },
                (e, ex) -> {
                    assert false : "should not have been invoked, no invalid settings";
                });
        assertSame(settings, Settings.EMPTY);
        settings =
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(
                Settings.builder().put("index.refresh_interval", "-200").build(),
                e -> {
                    assert false : "should not have been invoked, no invalid settings";
                },
                (e, ex) -> {
                    assertThat(e.getKey()).isEqualTo("index.refresh_interval");
                    assertThat(e.getValue()).isEqualTo("-200");
                    assertThat(ex.getMessage()).contains(
                        "failed to parse setting [index.refresh_interval] with value [-200] as a time value: negative durations are not supported");
                });
        assertThat(settings.get("archived.index.refresh_interval")).isEqualTo("-200");
        assertNull(settings.get("index.refresh_interval"));

        Settings prevSettings = settings; // no double archive
        settings =
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(
                prevSettings,
                e -> {
                    assert false : "should not have been invoked, no unknown settings";
                },
                (e, ex) -> {
                    assert false : "should not have been invoked, no invalid settings";
                });
        assertSame(prevSettings, settings);

        settings =
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(
                Settings.builder()
                    .put("index.version.created", Version.CURRENT.internalId) // private setting
                    .put("index.unknown", "foo")
                    .put("index.refresh_interval", "2s").build(),
                e -> {
                    assertThat(e.getKey()).isEqualTo("index.unknown");
                    assertThat(e.getValue()).isEqualTo("foo");
                },
                (e, ex) -> {
                    assert false : "should not have been invoked, no invalid settings";
                });

        assertThat(settings.get("archived.index.unknown")).isEqualTo("foo");
        assertThat(settings.get("index.version.created")).isEqualTo(Integer.toString(Version.CURRENT.internalId));
        assertThat(settings.get("index.refresh_interval")).isEqualTo("2s");
    }

    @Test
    public void testUpdateSoftDeletesFails() {
        IndexScopedSettings settings = new IndexScopedSettings(Settings.EMPTY,
                                                               IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        assertThatThrownBy(() -> settings.updateSettings(
            Settings.builder().put("index.soft_deletes.enabled", randomBoolean()).build(),
            Settings.builder(),
            Settings.builder(),
            "index")
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("final index setting [index.soft_deletes.enabled], not updateable");
    }

    @Test
    public void testUpdateTranslogRetentionSettingsWithSoftDeletesDisabled() {
        Settings.Builder settings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);

        TimeValue ageSetting = TimeValue.timeValueHours(12);
        if (randomBoolean()) {
            ageSetting = randomBoolean() ? TimeValue.MINUS_ONE : TimeValue.timeValueSeconds(randomIntBetween(0, 60));
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), ageSetting);
        }
        ByteSizeValue sizeSetting = new ByteSizeValue(512, ByteSizeUnit.MB);
        if (randomBoolean()) {
            sizeSetting = randomBoolean() ? new ByteSizeValue(-1) : new ByteSizeValue(randomIntBetween(0, 1024));
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), sizeSetting);
        }
        IndexMetadata metaData = newIndexMeta("index", settings.build());
        IndexSettings indexSettings = new IndexSettings(metaData, Settings.EMPTY);
        assertThat(indexSettings.getTranslogRetentionAge()).isEqualTo(ageSetting);
        assertThat(indexSettings.getTranslogRetentionSize()).isEqualTo(sizeSetting);

        Settings.Builder newSettings = Settings.builder().put(settings.build());
        if (randomBoolean()) {
            ageSetting = randomBoolean() ? TimeValue.MINUS_ONE : TimeValue.timeValueSeconds(randomIntBetween(0, 60));
            newSettings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), ageSetting);
        }
        if (randomBoolean()) {
            sizeSetting = randomBoolean() ? new ByteSizeValue(-1) : new ByteSizeValue(randomIntBetween(0, 1024));
            newSettings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), sizeSetting);
        }
        indexSettings.updateIndexMetadata(newIndexMeta("index", newSettings.build()));
        assertThat(indexSettings.getTranslogRetentionAge()).isEqualTo(ageSetting);
        assertThat(indexSettings.getTranslogRetentionSize()).isEqualTo(sizeSetting);
    }

    @Test
    public void testIgnoreTranslogRetentionSettingsIfSoftDeletesEnabled() {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_4_3_0, Version.CURRENT));
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 1024) + "b");
        }
        IndexMetadata metaData = newIndexMeta("index", settings.build());
        IndexSettings indexSettings = new IndexSettings(metaData, Settings.EMPTY);
        assertThat(indexSettings.getTranslogRetentionAge().millis()).isEqualTo(-1L);
        assertThat(indexSettings.getTranslogRetentionSize().getBytes()).isEqualTo(-1L);

        Settings.Builder newSettings = Settings.builder().put(settings.build());
        if (randomBoolean()) {
            newSettings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            newSettings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 1024) + "b");
        }
        indexSettings.updateIndexMetadata(newIndexMeta("index", newSettings.build()));
        assertThat(indexSettings.getTranslogRetentionAge().millis()).isEqualTo(-1L);
        assertThat(indexSettings.getTranslogRetentionSize().getBytes()).isEqualTo(-1L);
    }


}

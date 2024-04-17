/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.node.Node;

import io.crate.common.unit.TimeValue;
import io.crate.types.DataTypes;

/**
 * This class encapsulates all index level settings and handles settings updates.
 * It's created per index and available to all index level classes and allows them to retrieve
 * the latest updated settings instance. Classes that need to listen to settings updates can register
 * a settings consumer at index creation via {@link IndexModule#addSettingsUpdateConsumer(Setting, Consumer)} that will
 * be called for each settings update.
 */
public final class IndexSettings {

    public static final Setting<TimeValue> INDEX_TRANSLOG_SYNC_INTERVAL_SETTING =
        Setting.timeSetting("index.translog.sync_interval", TimeValue.timeValueSeconds(5), TimeValue.timeValueMillis(100),
            Property.Dynamic, Property.IndexScope, Property.ReplicatedIndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_IDLE_AFTER =
        Setting.timeSetting("index.search.idle.after", TimeValue.timeValueSeconds(30),
            TimeValue.timeValueMinutes(0), Property.IndexScope, Property.Dynamic);
    public static final Setting<Translog.Durability> INDEX_TRANSLOG_DURABILITY_SETTING =
        new Setting<>("index.translog.durability",
                      Translog.Durability.REQUEST.name(),
                      (value) -> Translog.Durability.valueOf(value.toUpperCase(Locale.ROOT)),
                      DataTypes.STRING,
                      Property.Dynamic,
                      Property.IndexScope,
                      Property.ReplicatedIndexScope);

    public static final Setting<String> INDEX_CHECK_ON_STARTUP = new Setting<>("index.shard.check_on_startup", "false", (s) -> {
        switch (s) {
            case "false":
            case "true":
            case "fix":
            case "checksum":
                return s;
            default:
                throw new IllegalArgumentException("unknown value for [index.shard.check_on_startup] must be one of [true, false, fix, checksum] but was: " + s);
        }
    }, DataTypes.STRING, Property.IndexScope);

    /**
     * Index setting describing for NGramTokenizer and NGramTokenFilter
     * the maximum difference between
     * max_gram (maximum length of characters in a gram) and
     * min_gram (minimum length of characters in a gram).
     * The default value is 1 as this is default difference in NGramTokenizer,
     * and is defensive as it prevents generating too many index terms.
     */
    public static final Setting<Integer> MAX_NGRAM_DIFF_SETTING =
        Setting.intSetting("index.max_ngram_diff", 1, 0, Property.Dynamic, Property.IndexScope, Property.ReplicatedIndexScope);

    /**
     * Index setting describing for ShingleTokenFilter
     * the maximum difference between
     * max_shingle_size and min_shingle_size.
     * The default value is 3 is defensive as it prevents generating too many tokens.
     */
    public static final Setting<Integer> MAX_SHINGLE_DIFF_SETTING =
        Setting.intSetting("index.max_shingle_diff", 3, 0, Property.Dynamic, Property.IndexScope, Property.ReplicatedIndexScope);

    public static final TimeValue DEFAULT_REFRESH_INTERVAL = new TimeValue(1, TimeUnit.SECONDS);
    public static final Setting<TimeValue> INDEX_REFRESH_INTERVAL_SETTING =
        Setting.timeSetting("index.refresh_interval", DEFAULT_REFRESH_INTERVAL, new TimeValue(-1, TimeUnit.MILLISECONDS),
            Property.Dynamic, Property.IndexScope, Property.ReplicatedIndexScope);
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING =
        Setting.byteSizeSetting("index.translog.flush_threshold_size", new ByteSizeValue(512, ByteSizeUnit.MB),
            /*
             * An empty translog occupies 55 bytes on disk. If the flush threshold is below this, the flush thread
             * can get stuck in an infinite loop as the shouldPeriodicallyFlush can still be true after flushing.
             * However, small thresholds are useful for testing so we do not add a large lower bound here.
             */
            new ByteSizeValue(Translog.DEFAULT_HEADER_SIZE_IN_BYTES + 1, ByteSizeUnit.BYTES),
            new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
            Property.Dynamic, Property.IndexScope, Property.ReplicatedIndexScope);


    /**
     * The minimum size of a merge that triggers a flush in order to free resources
     */
    public static final Setting<ByteSizeValue> INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING =
        Setting.byteSizeSetting("index.flush_after_merge", new ByteSizeValue(512, ByteSizeUnit.MB),
                                new ByteSizeValue(0, ByteSizeUnit.BYTES), // always flush after merge
                                new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES), // never flush after merge
                                Property.Dynamic, Property.IndexScope);

    /**
     * The maximum size of a translog generation. This is independent of the maximum size of
     * translog operations that have not been flushed.
     */
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING =
            Setting.byteSizeSetting(
                    "index.translog.generation_threshold_size",
                    new ByteSizeValue(64, ByteSizeUnit.MB),
                    /*
                     * An empty translog occupies 55 bytes on disk. If the generation threshold is
                     * below this, the flush thread can get stuck in an infinite loop repeatedly
                     * rolling the generation as every new generation will already exceed the
                     * generation threshold. However, small thresholds are useful for testing so we
                     * do not add a large lower bound here.
                     */
                    new ByteSizeValue(Translog.DEFAULT_HEADER_SIZE_IN_BYTES + 1, ByteSizeUnit.BYTES),
                    new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
                    Property.Dynamic, Property.IndexScope);

    /**
     * Index setting to enable / disable deletes garbage collection.
     * This setting is realtime updateable
     */
    public static final TimeValue DEFAULT_GC_DELETES = TimeValue.timeValueSeconds(60);
    public static final Setting<TimeValue> INDEX_GC_DELETES_SETTING =
        Setting.timeSetting("index.gc_deletes", DEFAULT_GC_DELETES, new TimeValue(-1, TimeUnit.MILLISECONDS), Property.Dynamic,
            Property.IndexScope);

    /**
     * Specifies if the index should use soft-delete instead of hard-delete for update/delete operations.
     * Soft-deletes are enabled by default for 4.3+ indices and mandatory for 5.0+ indices.
     */
    public static final Setting<Boolean> INDEX_SOFT_DELETES_SETTING = Setting.boolSetting(
        "index.soft_deletes.enabled",
        true,
        Property.IndexScope,
        Property.Final,
        Property.Deprecated
    );

    /**
     * Controls how many soft-deleted documents will be kept around before being merged away. Keeping more deleted
     * documents increases the chance of operation-based recoveries and allows querying a longer history of documents.
     * If soft-deletes is enabled, an engine by default will retain all operations up to the global checkpoint.
     **/
    public static final Setting<Long> INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING =
        Setting.longSetting("index.soft_deletes.retention.operations", 0, 0, Property.IndexScope, Property.Dynamic);

    /**
     * Controls how long translog files that are no longer needed for persistence reasons
     * will be kept around before being deleted. Keeping more files is useful to increase
     * the chance of ops based recoveries for indices with soft-deletes disabled.
     * This setting will be ignored if soft-deletes is used in peer recoveries (default in CrateDB 4.3).
     **/
    public static final Setting<TimeValue> INDEX_TRANSLOG_RETENTION_AGE_SETTING =
        Setting.timeSetting("index.translog.retention.age",
            settings -> shouldDisableTranslogRetention(settings) ? TimeValue.MINUS_ONE : TimeValue.timeValueHours(12),
            TimeValue.MINUS_ONE, Property.Dynamic, Property.IndexScope, Property.Deprecated);

    /**
     * Controls how many translog files that are no longer needed for persistence reasons
     * will be kept around before being deleted. Keeping more files is useful to increase
     * the chance of ops based recoveries for indices with soft-deletes disabled.
     * This setting will be ignored if soft-deletes is used in peer recoveries (default in CrateDB 4.3).
     **/
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_RETENTION_SIZE_SETTING =
        Setting.byteSizeSetting("index.translog.retention.size",
            settings -> shouldDisableTranslogRetention(settings) ? "-1" : "512MB",
            Property.Dynamic, Property.IndexScope, Property.Deprecated);

    /**
     * Controls the number of translog files that are no longer needed for persistence reasons will be kept around before being deleted.
     * This is a safeguard making sure that the translog deletion policy won't keep too many translog files especially when they're small.
     * This setting is intentionally not registered, it is only used in tests
     **/
    public static final Setting<Integer> INDEX_TRANSLOG_RETENTION_TOTAL_FILES_SETTING =
        Setting.intSetting("index.translog.retention.total_files", 100, 0, Setting.Property.IndexScope);

    /**
     * Controls the maximum length of time since a retention lease is created or renewed before it is considered expired.
     */
    public static final Setting<TimeValue> INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING =
        Setting.timeSetting(
            "index.soft_deletes.retention_lease.period",
            TimeValue.timeValueHours(12),
            TimeValue.ZERO,
            Property.Dynamic,
            Property.IndexScope,
            Property.ReplicatedIndexScope
        );


    /**
     * The maximum number of refresh listeners allows on this shard.
     */
    public static final Setting<Integer> MAX_REFRESH_LISTENERS_PER_SHARD = Setting.intSetting("index.max_refresh_listeners", 1000, 0,
            Property.Dynamic, Property.IndexScope);

    /**
     * Determines a balance between file-based and operations-based peer recoveries. The number of operations that will be used in an
     * operations-based peer recovery is limited to this proportion of the total number of documents in the shard (including deleted
     * documents) on the grounds that a file-based peer recovery may copy all of the documents in the shard over to the new peer, but is
     * significantly faster than replaying the missing operations on the peer, so once a peer falls far enough behind the primary it makes
     * more sense to copy all the data over again instead of replaying history.
     *
     * Defaults to retaining history for up to 10% of the documents in the shard. This can only be changed in tests, since this setting is
     * intentionally unregistered.
     */
    public static final Setting<Double> FILE_BASED_RECOVERY_THRESHOLD_SETTING
        = Setting.doubleSetting("index.recovery.file_based_threshold", 0.1d, 0.0d, Setting.Property.IndexScope);

    private final Index index;
    private final Version version;
    private final Logger logger;
    private final String nodeName;
    private final Settings nodeSettings;
    private final int numberOfShards;
    // volatile fields are updated via #updateIndexMetadata(IndexMetadata) under lock
    private volatile Settings settings;
    private volatile IndexMetadata indexMetadata;
    private volatile Translog.Durability durability;
    private volatile TimeValue syncInterval;
    private volatile TimeValue refreshInterval;
    private volatile ByteSizeValue flushThresholdSize;
    private volatile TimeValue translogRetentionAge;
    private volatile ByteSizeValue translogRetentionSize;
    private volatile ByteSizeValue flushAfterMergeThresholdSize;
    private volatile ByteSizeValue generationThresholdSize;
    private final MergeSchedulerConfig mergeSchedulerConfig;
    private final MergePolicyConfig mergePolicyConfig;
    private final IndexScopedSettings scopedSettings;
    private long gcDeletesInMillis = DEFAULT_GC_DELETES.millis();
    private final boolean softDeleteEnabled;
    private volatile long softDeleteRetentionOperations;

    private volatile long retentionLeaseMillis;

    /**
     * The maximum age of a retention lease before it is considered expired.
     *
     * @return the maximum age
     */
    public long getRetentionLeaseMillis() {
        return retentionLeaseMillis;
    }

    private void setRetentionLeaseMillis(final TimeValue retentionLease) {
        this.retentionLeaseMillis = retentionLease.millis();
    }

    private volatile int maxNgramDiff;
    private volatile int maxShingleDiff;
    private volatile TimeValue searchIdleAfter;

    /**
     * The maximum number of refresh listeners allows on this shard.
     */
    private volatile int maxRefreshListeners;

    /**
     * Creates a new {@link IndexSettings} instance. The given node settings will be merged with the settings in the metadata
     * while index level settings will overwrite node settings.
     *
     * @param indexMetadata the index metadata this settings object is associated with
     * @param nodeSettings the nodes settings this index is allocated on.
     */
    public IndexSettings(final IndexMetadata indexMetadata, final Settings nodeSettings) {
        this(indexMetadata, nodeSettings, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS);
    }

    /**
     * Creates a new {@link IndexSettings} instance. The given node settings will be merged with the settings in the metadata
     * while index level settings will overwrite node settings.
     *
     * @param indexMetadata the index metadata this settings object is associated with
     * @param nodeSettings the nodes settings this index is allocated on.
     */
    public IndexSettings(final IndexMetadata indexMetadata, final Settings nodeSettings, IndexScopedSettings indexScopedSettings) {
        scopedSettings = indexScopedSettings.copy(nodeSettings, indexMetadata);
        this.nodeSettings = nodeSettings;
        this.settings = Settings.builder().put(nodeSettings).put(indexMetadata.getSettings()).build();
        this.index = indexMetadata.getIndex();
        version = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings);
        logger = Loggers.getLogger(getClass(), index);
        nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.indexMetadata = indexMetadata;
        numberOfShards = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, null);
        this.durability = scopedSettings.get(INDEX_TRANSLOG_DURABILITY_SETTING);
        syncInterval = INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.get(settings);
        refreshInterval = scopedSettings.get(INDEX_REFRESH_INTERVAL_SETTING);
        flushThresholdSize = scopedSettings.get(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING);
        generationThresholdSize = scopedSettings.get(INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING);
        flushAfterMergeThresholdSize = scopedSettings.get(INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING);
        mergeSchedulerConfig = new MergeSchedulerConfig(this);
        gcDeletesInMillis = scopedSettings.get(INDEX_GC_DELETES_SETTING).millis();
        softDeleteEnabled = scopedSettings.get(INDEX_SOFT_DELETES_SETTING);
        softDeleteRetentionOperations = scopedSettings.get(INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING);
        retentionLeaseMillis = scopedSettings.get(INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING).millis();
        maxNgramDiff = scopedSettings.get(MAX_NGRAM_DIFF_SETTING);
        maxShingleDiff = scopedSettings.get(MAX_SHINGLE_DIFF_SETTING);
        maxRefreshListeners = scopedSettings.get(MAX_REFRESH_LISTENERS_PER_SHARD);
        this.mergePolicyConfig = new MergePolicyConfig(logger, this);
        searchIdleAfter = scopedSettings.get(INDEX_SEARCH_IDLE_AFTER);
        setTranslogRetentionAge(scopedSettings.get(INDEX_TRANSLOG_RETENTION_AGE_SETTING));
        setTranslogRetentionSize(scopedSettings.get(INDEX_TRANSLOG_RETENTION_SIZE_SETTING));

        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING, mergePolicyConfig::setNoCFSRatio);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING, mergePolicyConfig::setDeletesPctAllowed);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING, mergePolicyConfig::setExpungeDeletesAllowed);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING, mergePolicyConfig::setFloorSegmentSetting);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING, mergePolicyConfig::setMaxMergesAtOnce);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING, mergePolicyConfig::setMaxMergedSegment);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING, mergePolicyConfig::setSegmentsPerTier);

        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING, MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING,
            mergeSchedulerConfig::setMaxThreadAndMergeCount);
        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.AUTO_THROTTLE_SETTING, mergeSchedulerConfig::setAutoThrottle);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_DURABILITY_SETTING, this::setTranslogDurability);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_SYNC_INTERVAL_SETTING, this::setTranslogSyncInterval);
        scopedSettings.addSettingsUpdateConsumer(MAX_NGRAM_DIFF_SETTING, this::setMaxNgramDiff);
        scopedSettings.addSettingsUpdateConsumer(MAX_SHINGLE_DIFF_SETTING, this::setMaxShingleDiff);
        scopedSettings.addSettingsUpdateConsumer(INDEX_GC_DELETES_SETTING, this::setGCDeletes);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING, this::setTranslogFlushThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(
            INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING,
            this::setFlushAfterMergeThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(
                INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING,
                this::setGenerationThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_RETENTION_AGE_SETTING, this::setTranslogRetentionAge);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_RETENTION_SIZE_SETTING, this::setTranslogRetentionSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
        scopedSettings.addSettingsUpdateConsumer(MAX_REFRESH_LISTENERS_PER_SHARD, this::setMaxRefreshListeners);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SEARCH_IDLE_AFTER, this::setSearchIdleAfter);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING, this::setSoftDeleteRetentionOperations);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING, this::setRetentionLeaseMillis);
    }

    private void setSearchIdleAfter(TimeValue searchIdleAfter) {
        this.searchIdleAfter = searchIdleAfter;
    }

    private void setTranslogFlushThresholdSize(ByteSizeValue byteSizeValue) {
        this.flushThresholdSize = byteSizeValue;
    }

    private void setFlushAfterMergeThresholdSize(ByteSizeValue byteSizeValue) {
        this.flushAfterMergeThresholdSize = byteSizeValue;
    }

    private void setTranslogRetentionSize(ByteSizeValue byteSizeValue) {
        if (shouldDisableTranslogRetention(settings) && byteSizeValue.getBytes() >= 0) {
            // ignore the translog retention settings if soft-deletes enabled
            this.translogRetentionSize = new ByteSizeValue(-1);
        } else {
            this.translogRetentionSize = byteSizeValue;
        }
    }

    private void setTranslogRetentionAge(TimeValue age) {
        if (shouldDisableTranslogRetention(settings) && age.millis() >= 0) {
            // ignore the translog retention settings if soft-deletes enabled
            this.translogRetentionAge = TimeValue.MINUS_ONE;
        } else {
            this.translogRetentionAge = age;
        }
    }

    private void setGenerationThresholdSize(final ByteSizeValue generationThresholdSize) {
        this.generationThresholdSize = generationThresholdSize;
    }

    private void setGCDeletes(TimeValue timeValue) {
        this.gcDeletesInMillis = timeValue.millis();
    }

    private void setRefreshInterval(TimeValue timeValue) {
        this.refreshInterval = timeValue;
    }

    /**
     * Returns the settings for this index. These settings contain the node and index level settings where
     * settings that are specified on both index and node level are overwritten by the index settings.
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * Returns the index this settings object belongs to
     */
    public Index getIndex() {
        return index;
    }

    /**
     * Returns the indexes UUID
     */
    public String getUUID() {
        return getIndex().getUUID();
    }

    /**
     * Returns <code>true</code> if the index has a custom data path
     */
    public boolean hasCustomDataPath() {
        return Strings.isNotEmpty(customDataPath());
    }

    /**
     * Returns the customDataPath for this index, if configured. <code>""</code> o.w.
     */
    public String customDataPath() {
        return IndexMetadata.INDEX_DATA_PATH_SETTING.get(settings);
    }

    /**
     * Returns the version the index was created on.
     * @see Version#indexCreated(Settings)
     */
    public Version getIndexVersionCreated() {
        return version;
    }

    /**
     * Returns the current node name
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Returns the current IndexMetadata for this index
     */
    public IndexMetadata getIndexMetadata() {
        return indexMetadata;
    }

    /**
     * Returns the number of shards this index has.
     */
    public int getNumberOfShards() {
        return numberOfShards;
    }

    /**
     * Returns the number of replicas this index has.
     */
    public int getNumberOfReplicas() {
        return settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, null);
    }

    /**
     * Returns the node settings. The settings returned from {@link #getSettings()} are a merged version of the
     * index settings and the node settings where node settings are overwritten by index settings.
     */
    public Settings getNodeSettings() {
        return nodeSettings;
    }

    /**
     * Updates the settings and index metadata and notifies all registered settings consumers with the new settings iff at least one setting has changed.
     *
     * @return <code>true</code> iff any setting has been updated otherwise <code>false</code>.
     */
    public synchronized boolean updateIndexMetadata(IndexMetadata indexMetadata) {
        final Settings newSettings = indexMetadata.getSettings();
        if (version.equals(Version.indexCreated(newSettings)) == false) {
            throw new IllegalArgumentException("version mismatch on settings update expected: " + version + " but was: " + Version.indexCreated(newSettings));
        }
        final String newUUID = newSettings.get(IndexMetadata.SETTING_INDEX_UUID, IndexMetadata.INDEX_UUID_NA_VALUE);
        if (newUUID.equals(getUUID()) == false) {
            throw new IllegalArgumentException("uuid mismatch on settings update expected: " + getUUID() + " but was: " + newUUID);
        }
        final String newRestoreUUID = newSettings.get(IndexMetadata.SETTING_HISTORY_UUID, IndexMetadata.INDEX_UUID_NA_VALUE);
        final String restoreUUID = this.settings.get(IndexMetadata.SETTING_HISTORY_UUID, IndexMetadata.INDEX_UUID_NA_VALUE);
        if (newRestoreUUID.equals(restoreUUID) == false) {
            throw new IllegalArgumentException("uuid mismatch on settings update expected: " + restoreUUID + " but was: " + newRestoreUUID);
        }
        this.indexMetadata = indexMetadata;
        final Settings newIndexSettings = Settings.builder().put(nodeSettings).put(newSettings).build();
        if (same(this.settings, newIndexSettings)) {
            // nothing to update, same settings
            return false;
        }
        scopedSettings.applySettings(newSettings);
        this.settings = newIndexSettings;
        return true;
    }

    /**
     * Compare the specified settings for equality.
     *
     * @param left  the left settings
     * @param right the right settings
     * @return true if the settings are the same, otherwise false
     */
    public static boolean same(final Settings left, final Settings right) {
        return left.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE)
                .equals(right.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE));
    }

    /**
     * Returns the translog durability for this index.
     */
    public Translog.Durability getTranslogDurability() {
        return durability;
    }

    private void setTranslogDurability(Translog.Durability durability) {
        this.durability = durability;
    }

    /**
     * Returns the translog sync interval. This is the interval in which the transaction log is asynchronously fsynced unless
     * the transaction log is fsyncing on every operations
     */
    public TimeValue getTranslogSyncInterval() {
        return syncInterval;
    }

    public void setTranslogSyncInterval(TimeValue translogSyncInterval) {
        this.syncInterval = translogSyncInterval;
    }

    /**
     * Returns this interval in which the shards of this index are asynchronously refreshed. {@code -1} means async refresh is disabled.
     */
    public TimeValue getRefreshInterval() {
        return refreshInterval;
    }

    /**
     * Returns the transaction log threshold size when to forcefully flush the index and clear the transaction log.
     */
    public ByteSizeValue getFlushThresholdSize() {
        return flushThresholdSize;
    }

    /**
     * Returns the merge threshold size when to forcefully flush the index and free resources.
     */
    public ByteSizeValue getFlushAfterMergeThresholdSize() {
        return flushAfterMergeThresholdSize;
    }


    /**
     * Returns the transaction log retention size which controls how much of the translog is kept around to allow for ops based recoveries
     */
    public ByteSizeValue getTranslogRetentionSize() {
        assert shouldDisableTranslogRetention(settings) == false || translogRetentionSize.getBytes() == -1L : translogRetentionSize;
        return translogRetentionSize;
    }

    /**
     * Returns the transaction log retention age which controls the maximum age (time from creation) that translog files will be kept around
     */
    public TimeValue getTranslogRetentionAge() {
        assert shouldDisableTranslogRetention(settings) == false || translogRetentionAge.millis() == -1L : translogRetentionSize;
        return translogRetentionAge;
    }

    /**
     * Returns the maximum number of translog files that that no longer required for persistence should be kept for peer recovery
     * when soft-deletes is disabled.
     */
    public int getTranslogRetentionTotalFiles() {
        return INDEX_TRANSLOG_RETENTION_TOTAL_FILES_SETTING.get(getSettings());
    }

    private static boolean shouldDisableTranslogRetention(Settings settings) {
        return INDEX_SOFT_DELETES_SETTING.get(settings)
            && IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings).onOrAfter(Version.V_4_3_0);
    }

    /**
     * Returns the generation threshold size. As sequence numbers can cause multiple generations to
     * be preserved for rollback purposes, we want to keep the size of individual generations from
     * growing too large to avoid excessive disk space consumption. Therefore, the translog is
     * automatically rolled to a new generation when the current generation exceeds this generation
     * threshold size.
     *
     * @return the generation threshold size
     */
    public ByteSizeValue getGenerationThresholdSize() {
        return generationThresholdSize;
    }

    /**
     * Returns the {@link MergeSchedulerConfig}
     */
    public MergeSchedulerConfig getMergeSchedulerConfig() {
        return mergeSchedulerConfig;
    }

    /**
     * Returns the maximum allowed difference between max and min length of ngram
     */
    public int getMaxNgramDiff() {
        return this.maxNgramDiff;
    }

    private void setMaxNgramDiff(int maxNgramDiff) {
        this.maxNgramDiff = maxNgramDiff;
    }

    /**
     * Returns the maximum allowed difference between max and min shingle_size
     */
    public int getMaxShingleDiff() {
        return this.maxShingleDiff;
    }

    private void setMaxShingleDiff(int maxShingleDiff) {
        this.maxShingleDiff = maxShingleDiff;
    }

    /**
     * Returns the GC deletes cycle in milliseconds.
     */
    public long getGcDeletesInMillis() {
        return gcDeletesInMillis;
    }

    /**
     * Returns the merge policy that should be used for this index.
     */
    public MergePolicy getMergePolicy() {
        return mergePolicyConfig.getMergePolicy();
    }

    public <T> T getValue(Setting<T> setting) {
        return scopedSettings.get(setting);
    }

    /**
     * The maximum number of refresh listeners allows on this shard.
     */
    public int getMaxRefreshListeners() {
        return maxRefreshListeners;
    }

    private void setMaxRefreshListeners(int maxRefreshListeners) {
        this.maxRefreshListeners = maxRefreshListeners;
    }

    public IndexScopedSettings getScopedSettings() {
        return scopedSettings;
    }

    /**
     * Returns true iff the refresh setting exists or in other words is explicitly set.
     */
    public boolean isExplicitRefresh() {
        return INDEX_REFRESH_INTERVAL_SETTING.exists(settings);
    }

    /**
     * Returns the time that an index shard becomes search idle unless it's accessed in between
     */
    public TimeValue getSearchIdleAfter() {
        return searchIdleAfter;
    }

    /**
     * Returns <code>true</code> if soft-delete is enabled.
     */
    public boolean isSoftDeleteEnabled() {
        return softDeleteEnabled;
    }

    private void setSoftDeleteRetentionOperations(long ops) {
        this.softDeleteRetentionOperations = ops;
    }

    /**
     * Returns the number of extra operations (i.e. soft-deleted documents) to be kept for recoveries and history purpose.
     */
    public long getSoftDeleteRetentionOperations() {
        return this.softDeleteRetentionOperations;
    }
}

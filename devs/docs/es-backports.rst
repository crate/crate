======================================
Backporting changes from Elasticsearch
======================================

With CrateDB 4.0.0, we integrated the Elasticsearch code and then pruned out
everything we don't use.

With CrateDB 4.2.0, we changed the module structure so that Elasticsearch and
CrateDB code is no longer split, but mixed within the same modules. The
majority is in the ``server`` component.

We still want to keep many of the components up-to-date, while other
components will converge more and more over time.

The areas where we want to stay close to upstream Elasticsearch are mostly:

- Engine
- Snapshotting
- Recovery
- Discovery and master election


Applied changesets
==================

Patches should be applied in order. A backport commit message should be
prefixed with ``bp:``. This allows us to verify later on whether a patch was
applied using something like this::

    sh$ git log --oneline --grep 'bp:'

If a patch is skipped, use the ``sp:`` prefix instead.

To see if there are new changes, you can use ``git log`` in the Elasticsearch
repository on the 7.10 branch. For example::

    git log --oneline 22ba759e1f3.. -- \
      server/src/main/java/org/elasticsearch/index/{engine,snapshots,store,translog,shard,seqno,mapper,codec}/ \
      server/src/main/java/org/elasticsearch/indices/recovery/ \
      server/src/main/java/org/elasticsearch/cluster/coordination \
      server/src/main/java/org/elasticsearch/cluster/routing \
      server/src/main/java/org/elasticsearch/transport \
      server/src/main/java/org/elasticsearch/gateway \
      server/src/main/java/org/elasticsearch/action/admin/cluster/health \
      server/src/main/java/org/elasticsearch/action/support/replication \
      server/src/main/java/org/elasticsearch/action/support/master \
      server/src/main/java/org/elasticsearch/snapshots \
      server/src/main/java/org/elasticsearch/repositories


Here ``4b16d50cd4b`` is the starting point, it shows any changes since then
that affected files in the given folder.

(Note that there may be changes before ``4b16d50cd4b`` that are missing.)

We should eventually bump the reference point once all patches have been
applied to a certain point.

Below lists the changesets that we applied. This should be updated whenever a
backport is made. If a patch is skipped because it is not applicable, it
should be crossed out as well.

- ``x`` to mark applied patches
- ``s`` for skipped patches if they are not applicable (for example, if the
  functionality is not present in CrateDB)
- ``sn`` for skipped patches which have no effect because the related sources
  do not exist anymore
- ``sa`` for skipped patches which have been applied already
- ``su`` for skipped patches which are unrelated to CrateDB
- ``d`` marks a delayed patch - a non-trivial change we should apply that
  does not affect too any components, so delaying it doesn't make applying
  other patches more difficult

- [ ] 20d2313d4b6 Ensure ReplicationOperation notify listener once (#68256)
- [ ] b8102b2e0f3 [DOCS] Fix response typo in transport message listener (#68125) (#68169)
- [ ] e9b798bdb15 [7.10] Make FilterAllocationDecider totally ignore tier-based allocation settings (#67019) (#67034)
- [x] 5000ec87caa Fix constructors of NoOpResult (#66269)
- [ ] 7e1fc6dc674 Adjust Cleanup Order of Internal State in SnapshotsService (#66225) (#66244)
- [ ] 8cbb9612d0f [7.10] Create AllocationDeciders in the main method of the ILM step (#65037) (8ac30f9a) (#66070)
- [ ] 16fae5d66d3 Also reroute after shard snapshot size fetch failure (#66008)
- [ ] 26d67c1662d Ensure notify when proxy connections disconnect (#65697)
- [ ] 745f527feac Deduplicate Index Meta Generations when Deserializing (#65619) (#65666)
- [x] 6bbeedc9321 Reset Deflater/Inflater after Use in DeflateCompressor (#65617) (#65646)
- [ ] 0137c1679bb Fix the earliest last modified age of translog stats (#64753)
- [s] fb84b6710d5 Restore use of default search and search_quote analyzers (#65491) (#65562)
- [s] 88993e763f9 Fix handling of null values in geo_point (#65307)
- [ ] feca22729cd [DOCS] Remove duplicated word in replica shard allocator comment (#65295) (#65317)
- [s] caf143f4a51 Unused boost parameter should not throw mapping exception (#64999) (#65014)
- [ ] d173ba6b2d7 Fix NPE in toString of FailedShard (#64770) (#64779)
- [ ] 33f703ef1f8 Fix up roles after rolling upgrade (#64693)
- [s] 51e9d6f2275 Revert Serializing Outbound Transport Messages on IO Threads (#64632) (#64654)
- [ ] dad3b26560c Fix Typo in Repository Exception Message (#64412) (#64434)
- [su] 2983584ef6e Fix #invariant Assertion in CacheFile (#64180) (#64264)
- [ ] a697d5edae1 Don't Generate an Index Setting History UUID unless it's Supported (#64164) (#64213)
- [su] e02561476ea Fix Broken Clone Snapshot CS Update (#64116) (#64159)
- [su] bdea16301d7 Fix testMasterFailoverDuringCloneStep1 (#63580) (#64127)
- [ ] 7ea44d20c3c Try to fix DiskThresholdDeciderIT (#63614) (#63721)
- [ ] 424b3137841 Adapt Shard Generation Assertion for 7.x (#63625) (#63642)
- [ ] 9015b50e1b8 Check docs limit before indexing on primary (#63273)
- [x] f70391c6cca Fix Broken Snapshot State Machine in Corner Case (#63534) (#63608)
- [x] 845ccc22646 [DOCS] Fix dup word in ShardRouting hashcode method. (#63452) (#63583)
- [x] 8499924e51e InternalSnapshotsInfoService should also removed failed snapshot shard size infos (#63492) (#63592)
- [s] 9e52513c7bf Add support for missing value fetchers. (#63585)
- [x] 56092b1a9fd Flush translog writer before adding new operation (#63505)
- [x] ab9b37a82c7 Fix test timeout for health on master failover (#63455)
- [s] ae2fc4118d2 Add factory methods for common value fetchers. (#63438)
- [s] c6b915c8e67 Make TextFieldMapper.FAST_PHRASE_SUFFIX private.
- [s] c4726a2cece Don't emit separate warnings for type filters (#63391)
- [x] 244f1a60f92 Selectively Add ClusterState Listeners Depending on Node Roles (#63223) (#63396)
- [x] eac99dd594a SnapshotShardSizeInfo should prefer default value when provided (#63390) (#63394)
- [x] dd4b0d85fe0 Write translog operation bytes to byte stream (#63298)
- [x] 64bbbaeef1a Do not block Translog add on file write (#63374)
- [s] f17ca18dfa8 Make array value parsing flag more robust. (#63371)
- [x] 87076c32e21 Determine shard size before allocating shards recovering from snapshots (#61906) (#63337)
- [s] ca68298e89c Remove MapperService argument from IndexFieldData.Builder#build (#63197) (#63311)
- [s] 7405af8060b Convert TypeFieldType to a constant field type (#63214)
- [x] d7f6812d788 Improve Snapshot Abort Efficiency (#62173) (#63297)
- [su] 25fbc014598 Retry CCR shard follow task when no seed node left (#63225)
- [su] 5c3a4c13ddd Clone Snapshot API (#61839) (#63291)
- [x] e91936512aa Refactor SnapshotsInProgress State Transitions (#60517) (#63266)
- [su] 860791260df Implement Shard Snapshot Clone Logic (#62771) (#63260)
- [x] cf75abb021f Optimize XContentParserUtils.ensureExpectedToken (#62691) (#63253)
- [su] 51d0ed1bf30 Prepare Snapshot Shard State Update Logic For Clone Logic (#62617) (#63255)
- [x] 89de9fdcf77 Cleanup Blobstore Repository Metadata Serialization (#62727) (#63249)
- [x] d13c1f50581 Fix Overly Strict Assertion in BlobStoreRepository (#63061) (#63236)
- [x] b4a1199e871 Uniquely associate term with update task during election (#62212)
- [x] c9baadd19bf Fix to actually throttle indexing when throttling is activated (#61768)
- [s] ba5574935e2 Remove dependency of Geometry queries with mapped type names (#63077) (#63110)
- [x] 8c6e197f510 Remove allocation id from engine (#62680)
- [s] e28750b001e Add parameter update and conflict tests to MapperTestCase (#62828) (#62902)
- [s] 862fab06d3a Share same existsQuery impl throughout mappers (#57607)
- [s] 5ca86d541c5 Move stored flag from TextSearchInfo to MappedFieldType (#62717) (#62770)
- [s] cb1dc5260fb Dedicated threadpool for system index writes (#62792)
- [s] 3f856d1c81a Prioritise recovery of system index shards (#62640)
- [s] a0df0fb074b Search - add case insensitive flag for "term" family of queries #61596 (#62661)
- [x] 0d5250c99b1 Add Trace Logging to File Restore (#62755) (#62761)
- [x] 13e28b85ff5 Speed up RepositoryData Serialization (#62684) (#62703)
- [s] 803f78ef055 Add field type for version strings (#59773) (#62692)
- [x] 9a77f41e554 Fix cluster health when closing (#61709)
- [s] 6a298970fdd [7.x] Allow metadata fields in the _source (#62616)
- [s] 17aabaed155 Fix warning on boost docs and warning message on non-implementing fieldmappers
- [s] 43ace5f80d7 Emit deprecation warnings when boosts are defined in mappings (#62623)
- [x] 9f5e95505bf Also abort ongoing file restores when snapshot restore is aborted (#62441) (#62607)
- [x] 06d5d360f92 Tidy up fillInStackTrace implementations (#62555)
- [s] 9bb7ce0b229 [7.x] Allocate new indices on "hot" or "content" tier depending on data stream inclusion (#62338) (#62557)
- [s] 91e23305295 Warn on badly-formed null values for date and IP field mappers (#62487)
- [x] e0a4a94985f Speed up merging when source is disabled. (#62443) (#62474)
- [x] 62dcc5b1ae1 Suppress stack in VersionConflictEngineException (#62433)
- [x] 5112c173194 Add WARN Logging on Slow Transport Message Handling (#62444) (#62521)
- [x] 14aec44cd86 Log if recovery affected by disconnect (#62437)
- [s] 24a24d050ab Implement fields fetch for runtime fields (backport of #61995) (#62416)
- [x] ffbc64bd109 Log WARN on Response Deserialization Failure (#62368) (#62388)
- [x] 95766da3452 Save Some Allocations when Working with ClusterState (#62060) (#62303)
- [x] 875af1c976f Remove Dead Variable in BlobStoreIndexShardSnapshots. (#62285) (#62295)
- [s] 808c8689ac9 Always include the matching node when resolving point in time  (#61658)
- [s] 3fc35aa76e6 Shard Search Scroll failures consistency (#62061)
- [s] 4d528e91a12 Ensure validation of the reader context is executed first (#61831)
- [s] 3d69b5c41e2 Introduce point in time APIs in x-pack basic (#61062)
- [x] 7b941a18e9d Optimize Snapshot Shard Status Update Handling (#62070) (#62219)
- [s] 6710104673d Fix Creating NOOP Tasks on SNAPSHOT Pool (#62152) (#62157)
- [x] ed4984a32e7 Remove Redundant Stream Wrapping from Compression (#62017) (#62132)
- [x] 075271758e3 Keep checkpoint file channel open across fsyncs (#61744)
- [s] 2bb5716b3dc Add repositories metering API (#62088)
- [s] bb0a583990e Allow enabling soft-deletes on restore from snapshot (#62018)
- [x] 3389d5ccb25 Introduce integ tests for high disk watermark (#60460)
- [x] 395538f5083 Improve Snapshot State Machine Performance (#62000) (#62049)
- [s] a295b0aa86f Fix null_value parsing for data_nanos field mapper (#61994)
- [s] 1799c0c5833 Convert completion, binary, boolean tests to MapperTestCase (#62004)
- [s] 0c8b4385777 Add support for runtime fields (#61776)
- [x] b26584dff89 Remove unused deciders in BalancedShardsAllocator (#62026)
- [s] 6d08b55d4e3 Simplify searchable snapshot shard allocation (#61911)
- [s] 66bb1eea982 Improve error messages on bad [format] and [null_value] params for date mapper (#61932)
- [s] 31c026f25cc upgrade to Lucene-8.7.0-snapshot-61ea26a (#61957) (#61974)
- [s] af01ccee93e Add specific test for serializing all mapping parameter values (#61844) (#61877)
- [s] d59343b4ba8 Allow [null] values in [null_value] (#61798) (#61807)
- [x] 3fd25bfa877 Fix Concurrent Snapshot Create+Delete + Delete Index (#61770) (#61773)
- [s] 5723b928d7d Remove Outdated Snapshot Docs (#61684) (#61728)
- [s] 1bfebd54ea7 [7.x] Allocate newly created indices on data_hot tier nodes (#61342) (#61650)
- [s] f769821bc80 Pass SearchLookup supplier through to fielddataBuilder (#61430) (#61638)
- [x] b866aaf81c0 Use int for number of parts in blob store (#61618)

Below lists deferred patches. In-between patches that we applied or skipped
are not listed anymore.

- [d] c2deb287f13 Add a cluster setting to disallow loading fielddata on _id field (#49166)
- [d] 725dda37ea5 Flush instead of synced-flush inactive shards (#49126) -- CrateDB 5.0
- [d] b8ce07b4cc5 Pre-sort shards based on the max/min value of the primary sort field (#49092)
- [d] a5f17fc2750 Add preflight check to dynamic mapping updates (#48817)
- [d] 2e7d62c27c9 Geo: improve handling of out of bounds points in linestrings (#47939)
- [d] 54d6da54320 [Java.time] Calculate week of a year with ISO rules (#48209)
- [d] 694373294fe Allow truncation of clean translog (#47866)
- [d] e3adedf610d Geo: implement proper handling of out of bounds geo points (#47734)
- [d] f9cb29450ec Geo: Fixes indexing of linestrings that go around the globe (#47471)
- [d] 8585d58b767 Provide better error when updating geo_shape field mapper settings (#47281)
- [d] 65374c9c010 Tidy up Store#trimUnsafeCommits (#47062)
- [d] 4ab71116688 Geo: fix indexing of west to east linestrings crossing the antimeridian (#46601)
- [d] fab31abbcc0 Log deprecation warning if es.transport.cname_in_publish_address property is specified (#45662)
- [d] e0a2558a4c3 transport.publish_address should contain CNAME (#45626)
- [d] 13a8835e5a8 Geo: Change order of parameter in Geometries to lon, lat (#45332)
- [d] 245cb348d35 Add per-socket keepalive options (#44055)
- [d] b07310022d2 [SPATIAL] New ShapeFieldMapper for indexing cartesian geometries (#44980)
- [d] 7e627d27e5c Geo: move indexShape to AbstractGeometryFieldMapper.Indexer (#44979)
- [d] 94b684630c8 [GEO] Refactor DeprecatedParameters in AbstractGeometryFieldMapper (#44923)
- [d] f603f06250a Geo: refactor geo mapper and query builder (#44884)
- [d] 321c2b86270 Force Merge should reject requests with `only_expunge_deletes` and `max_num_segments` set (#44761)
- [d] fd54e3e8244 Remove support for old translog checkpoint formats (#44272)
- [d] c8ae530e7a6 Don't use index_phrases on graph queries (#44340)
- [d] 33ad7928fbb Geo: extract dateline handling logic from ShapeBuilders (#44187)
- [d] e28fb1f0658 Fix index_prefix sub field name on nested text fields (#43862)
- [d] 56a662ed288 Remove Support for VERSION_CHECKPOINTS Translogs (#42782)
- [d] 6e39433cd53 Remove "nodes/0" folder prefix from data path (#42489)
- [d] 3af0c1746b3 Expose external refreshes through the stats API (#38643)
- [d] ef18d3fb5b2 Add analysis modes to restrict token filter use contexts (#36103)

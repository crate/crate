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

package io.crate.es.test;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import io.crate.es.ElasticsearchException;
import io.crate.es.action.support.replication.ReplicationResponse.ShardInfo;
import io.crate.es.action.support.replication.ReplicationResponse.ShardInfo.Failure;
import io.crate.es.cluster.block.ClusterBlockException;
import io.crate.es.common.bytes.BytesArray;
import io.crate.es.common.bytes.BytesReference;
import io.crate.es.common.collect.Tuple;
import io.crate.es.common.xcontent.ToXContent;
import io.crate.es.common.xcontent.XContentBuilder;
import io.crate.es.common.xcontent.XContentFactory;
import io.crate.es.common.xcontent.XContentParser;
import io.crate.es.common.xcontent.XContentType;
import io.crate.es.discovery.DiscoverySettings;
import io.crate.es.index.shard.IndexShardRecoveringException;
import io.crate.es.index.shard.ShardId;
import io.crate.es.index.shard.ShardNotFoundException;
import io.crate.es.rest.RestStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomAsciiLettersOfLength;
import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomUnicodeOfLengthBetween;
import static java.util.Collections.singleton;
import static io.crate.es.cluster.metadata.IndexMetaData.INDEX_UUID_NA_VALUE;
import static io.crate.es.test.ESTestCase.randomFrom;

public final class RandomObjects {

    private RandomObjects() {

    }

    /**
     * Returns a tuple containing random stored field values and their corresponding expected values once printed out
     * via {@link ToXContent#toXContent(XContentBuilder, ToXContent.Params)} and parsed back via
     * {@link XContentParser#objectText()}.
     * Generates values based on what can get printed out. Stored fields values are retrieved from lucene and converted via
     * {@link io.crate.es.index.mapper.MappedFieldType#valueForDisplay(Object)} to either strings, numbers or booleans.
     *
     * @param random Random generator
     * @param xContentType the content type, used to determine what the expected values are for float numbers.
     */
    public static Tuple<List<Object>, List<Object>> randomStoredFieldValues(Random random, XContentType xContentType) {
        int numValues = randomIntBetween(random, 1, 5);
        List<Object> originalValues = new ArrayList<>();
        List<Object> expectedParsedValues = new ArrayList<>();
        int dataType = randomIntBetween(random, 0, 8);
        for (int i = 0; i < numValues; i++) {
            switch(dataType) {
                case 0:
                    long randomLong = random.nextLong();
                    originalValues.add(randomLong);
                    expectedParsedValues.add(randomLong);
                    break;
                case 1:
                    int randomInt = random.nextInt();
                    originalValues.add(randomInt);
                    expectedParsedValues.add(randomInt);
                    break;
                case 2:
                    Short randomShort = (short) random.nextInt();
                    originalValues.add(randomShort);
                    expectedParsedValues.add(randomShort.intValue());
                    break;
                case 3:
                    Byte randomByte = (byte)random.nextInt();
                    originalValues.add(randomByte);
                    expectedParsedValues.add(randomByte.intValue());
                    break;
                case 4:
                    double randomDouble = random.nextDouble();
                    originalValues.add(randomDouble);
                    expectedParsedValues.add(randomDouble);
                    break;
                case 5:
                    Float randomFloat = random.nextFloat();
                    originalValues.add(randomFloat);
                    if (xContentType == XContentType.SMILE) {
                        //with SMILE we get back a double (this will change in Jackson 2.9 where it will return a Float)
                        expectedParsedValues.add(randomFloat.doubleValue());
                    } else {
                        //with JSON AND YAML we get back a double, but with float precision.
                        expectedParsedValues.add(Double.parseDouble(randomFloat.toString()));
                    }
                    break;
                case 6:
                    boolean randomBoolean = random.nextBoolean();
                    originalValues.add(randomBoolean);
                    expectedParsedValues.add(randomBoolean);
                    break;
                case 7:
                    String randomString = random.nextBoolean() ? RandomStrings.randomAsciiLettersOfLengthBetween(random, 3, 10) :
                            randomUnicodeOfLengthBetween(random, 3, 10);
                    originalValues.add(randomString);
                    expectedParsedValues.add(randomString);
                    break;
                case 8:
                    byte[] randomBytes = RandomStrings.randomUnicodeOfLengthBetween(random, 10, 50).getBytes(StandardCharsets.UTF_8);
                    BytesArray randomBytesArray = new BytesArray(randomBytes);
                    originalValues.add(randomBytesArray);
                    if (xContentType == XContentType.JSON || xContentType == XContentType.YAML) {
                        //JSON and YAML write the base64 format
                        expectedParsedValues.add(Base64.getEncoder().encodeToString(randomBytes));
                    } else {
                        //SMILE and CBOR write the original bytes as they support binary format
                        expectedParsedValues.add(randomBytesArray);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return Tuple.tuple(originalValues, expectedParsedValues);
    }

    /**
     * Returns a random source containing a random number of fields, objects and array, with maximum depth 5.
     *
     * @param random Random generator
     */
    public static BytesReference randomSource(Random random) {
        //the source can be stored in any format and eventually converted when retrieved depending on the format of the response
        return randomSource(random, RandomPicks.randomFrom(random, XContentType.values()));
    }

    /**
     * Returns a random source in a given XContentType containing a random number of fields, objects and array, with maximum depth 5.
     * The minimum number of fields per object is 1.
     *
     * @param random Random generator
     */
    public static BytesReference randomSource(Random random, XContentType xContentType) {
        return randomSource(random, xContentType, 1);
    }

    /**
     * Returns a random source in a given XContentType containing a random number of fields, objects and array, with maximum depth 5.
     * The minimum number of fields per object is provided as an argument.
     *
     * @param random Random generator
     */
    public static BytesReference randomSource(Random random, XContentType xContentType, int minNumFields) {
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType)) {
            builder.startObject();
            addFields(random, builder, minNumFields, 0);
            builder.endObject();
            return BytesReference.bytes(builder);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Randomly adds fields, objects, or arrays to the provided builder. The maximum depth is 5.
     */
    private static void addFields(Random random, XContentBuilder builder, int minNumFields, int currentDepth) throws IOException {
        int numFields = randomIntBetween(random, minNumFields, 5);
        for (int i = 0; i < numFields; i++) {
            if (currentDepth < 5 && random.nextInt(100) >= 70) {
                if (random.nextBoolean()) {
                    builder.startObject(RandomStrings.randomAsciiLettersOfLengthBetween(random, 6, 10));
                    addFields(random, builder, minNumFields, currentDepth + 1);
                    builder.endObject();
                } else {
                    builder.startArray(RandomStrings.randomAsciiLettersOfLengthBetween(random, 6, 10));
                    int numElements = randomIntBetween(random, 1, 5);
                    boolean object = random.nextBoolean();
                    int dataType = -1;
                    if (object == false) {
                        dataType = randomDataType(random);
                    }
                    for (int j = 0; j < numElements; j++) {
                        if (object) {
                            builder.startObject();
                            addFields(random, builder, minNumFields, 5);
                            builder.endObject();
                        } else {
                            builder.value(randomFieldValue(random, dataType));
                        }
                    }
                    builder.endArray();
                }
            } else {
                builder.field(RandomStrings.randomAsciiLettersOfLengthBetween(random, 6, 10),
                        randomFieldValue(random, randomDataType(random)));
            }
        }
    }

    private static int randomDataType(Random random) {
        return randomIntBetween(random, 0, 3);
    }

    private static Object randomFieldValue(Random random, int dataType) {
        switch(dataType) {
            case 0:
                return RandomStrings.randomAsciiLettersOfLengthBetween(random, 3, 10);
            case 1:
                return RandomStrings.randomAsciiLettersOfLengthBetween(random, 3, 10);
            case 2:
                return random.nextLong();
            case 3:
                return random.nextDouble();
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Returns a tuple that contains a randomized {@link ShardInfo} value (left side) and its corresponding
     * value (right side) after it has been printed out as a {@link ToXContent} and parsed back using a parsing
     * method like {@link ShardInfo#fromXContent(XContentParser)}. The ShardInfo randomly contains shard failures.
     *
     * @param random Random generator
     */
    public static Tuple<ShardInfo, ShardInfo> randomShardInfo(Random random) {
        return randomShardInfo(random, random.nextBoolean());
    }

    /**
     * Returns a tuple that contains a randomized {@link ShardInfo} value (left side) and its corresponding
     * value (right side) after it has been printed out as a {@link ToXContent} and parsed back using a parsing
     * method like {@link ShardInfo#fromXContent(XContentParser)}. A `withShardFailures` parameter indicates if
     * the randomized ShardInfo must or must not contain shard failures.
     *
     * @param random            Random generator
     * @param withShardFailures indicates if the generated ShardInfo must contain shard failures
     */
    public static Tuple<ShardInfo, ShardInfo> randomShardInfo(Random random, boolean withShardFailures) {
        int total = randomIntBetween(random, 1, 10);
        if (withShardFailures == false) {
            return Tuple.tuple(new ShardInfo(total, total), new ShardInfo(total, total));
        }

        int successful = randomIntBetween(random, 1, Math.max(1, (total - 1)));
        int failures = Math.max(1, (total - successful));

        Failure[] actualFailures = new Failure[failures];
        Failure[] expectedFailures = new Failure[failures];

        for (int i = 0; i < failures; i++) {
            Tuple<Failure, Failure> failure = randomShardInfoFailure(random);
            actualFailures[i] = failure.v1();
            expectedFailures[i] = failure.v2();
        }
        return Tuple.tuple(new ShardInfo(total, successful, actualFailures), new ShardInfo(total, successful, expectedFailures));
    }

    /**
     * Returns a tuple that contains a randomized {@link Failure} value (left side) and its corresponding
     * value (right side) after it has been printed out as a {@link ToXContent} and parsed back using a parsing
     * method like {@link ShardInfo.Failure#fromXContent(XContentParser)}.
     *
     * @param random Random generator
     */
    private static Tuple<Failure, Failure> randomShardInfoFailure(Random random) {
        String index = randomAsciiLettersOfLength(random, 5);
        String indexUuid = randomAsciiLettersOfLength(random, 5);
        int shardId = randomIntBetween(random, 1, 10);
        String nodeId = randomAsciiLettersOfLength(random, 5);
        RestStatus status = randomFrom(random, RestStatus.INTERNAL_SERVER_ERROR, RestStatus.FORBIDDEN, RestStatus.NOT_FOUND);
        boolean primary = random.nextBoolean();
        ShardId shard = new ShardId(index, indexUuid, shardId);

        Exception actualException;
        ElasticsearchException expectedException;

        int type = randomIntBetween(random, 0, 3);
        switch (type) {
            case 0:
                actualException = new ClusterBlockException(singleton(DiscoverySettings.NO_MASTER_BLOCK_WRITES));
                expectedException = new ElasticsearchException("Elasticsearch exception [type=cluster_block_exception, " +
                        "reason=blocked by: [SERVICE_UNAVAILABLE/2/no master];]");
                break;
            case 1:
                actualException = new ShardNotFoundException(shard);
                expectedException = new ElasticsearchException("Elasticsearch exception [type=shard_not_found_exception, " +
                        "reason=no such shard]");
                expectedException.setShard(shard);
                break;
            case 2:
                actualException = new IllegalArgumentException("Closed resource", new RuntimeException("Resource"));
                expectedException = new ElasticsearchException("Elasticsearch exception [type=illegal_argument_exception, " +
                        "reason=Closed resource]",
                        new ElasticsearchException("Elasticsearch exception [type=runtime_exception, reason=Resource]"));
                break;
            case 3:
                actualException = new IndexShardRecoveringException(shard);
                expectedException = new ElasticsearchException("Elasticsearch exception [type=index_shard_recovering_exception, " +
                        "reason=CurrentState[RECOVERING] Already recovering]");
                expectedException.setShard(shard);
                break;
            default:
                throw new UnsupportedOperationException("No randomized exceptions generated for type [" + type + "]");
        }

        Failure actual = new Failure(shard, nodeId, actualException, status, primary);
        Failure expected = new Failure(new ShardId(index, INDEX_UUID_NA_VALUE, shardId), nodeId, expectedException, status, primary);

        return Tuple.tuple(actual, expected);
    }
}

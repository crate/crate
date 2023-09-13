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

package org.elasticsearch.test;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomUnicodeOfLengthBetween;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import io.crate.common.collections.Tuple;

public final class RandomObjects {

    private RandomObjects() {

    }

    /**
     * Returns a tuple containing random stored field values and their corresponding expected values once printed out
     * via {@link org.elasticsearch.common.xcontent.ToXContent#toXContent(XContentBuilder, ToXContent.Params)} and parsed back via
     * {@link org.elasticsearch.common.xcontent.XContentParser#objectText()}.
     * Generates values based on what can get printed out. Stored fields values are retrieved from lucene and converted via
     * {@link org.elasticsearch.index.mapper.MappedFieldType#valueForDisplay(Object)} to either strings, numbers or booleans.
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
        return new Tuple<>(originalValues, expectedParsedValues);
    }

    /**
     * Returns a random source containing a random number of fields, objects and array, with maximum depth 5.
     * The minimum number of fields per object is 1.
     *
     * @param random Random generator
     */
    public static BytesReference randomSource(Random random) {
        return randomSource(random, 1);
    }

    /**
     * Returns a random source in a given XContentType containing a random number of fields, objects and array, with maximum depth 5.
     * The minimum number of fields per object is provided as an argument.
     *
     * @param random Random generator
     */
    public static BytesReference randomSource(Random random, int minNumFields) {
        try (XContentBuilder builder = JsonXContent.builder()) {
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
}

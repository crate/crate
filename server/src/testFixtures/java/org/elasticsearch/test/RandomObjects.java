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

import java.io.IOException;
import java.util.Random;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

public final class RandomObjects {

    private RandomObjects() {

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

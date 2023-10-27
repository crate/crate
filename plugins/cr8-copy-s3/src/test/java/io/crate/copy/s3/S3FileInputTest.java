/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.copy.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import io.crate.copy.s3.common.S3ClientHelper;
import io.crate.copy.s3.common.S3URI;
import software.amazon.awssdk.services.s3.S3Client;


public class S3FileInputTest extends ESTestCase {

    private static S3FileInput s3FileInput;
    private static List<S3ObjectSummary> listObjectSummaries;

    private static final ObjectListing OBJECT_LISTING = mock(ObjectListing.class);
    private static final S3ClientHelper CLIENT_BUILDER = mock(S3ClientHelper.class);
    private static final AmazonS3 S_3 = mock(AmazonS3.class);

    private static final String BUCKET_NAME = "fakeBucket";
    private static final String PREFIX = "prefix/";
    private static final String PROTOCOL = "http";


    @BeforeClass
    public static void setUpClass() throws Exception {
        var globbedUri = new URI("s3://fakeBucket/prefix/*");
        S3URI preGlobUri = S3URI.toS3URI((new URI("s3://fakeBucket/prefix/")));
        s3FileInput = new S3FileInput(CLIENT_BUILDER, globbedUri, PROTOCOL);

        when(S_3.listObjects(BUCKET_NAME, PREFIX)).thenReturn(OBJECT_LISTING);
        // TODO
        when(CLIENT_BUILDER.client(preGlobUri, PROTOCOL)).thenReturn(mock(S3Client.class));
    }

    @Test
    public void testListListUrlsWhenEmptyKeysIsListed() throws Exception {
        S3ObjectSummary path = new S3ObjectSummary();
        path.setBucketName(BUCKET_NAME);
        path.setKey("prefix/");
        listObjectSummaries = objectSummaries();
        listObjectSummaries.add(path);

        when(OBJECT_LISTING.getObjectSummaries()).thenReturn(listObjectSummaries);

        List<URI> uris = s3FileInput.expandUri();
        assertThat(uris).hasSize(2);
        assertThat(uris.get(0)).hasToString("s3:///fakeBucket/prefix/test1.json.gz");
        assertThat(uris.get(1)).hasToString("s3:///fakeBucket/prefix/test2.json.gz");
    }

    @Test
    public void testListListUrlsWithCorrectKeys() throws Exception {
        when(OBJECT_LISTING.getObjectSummaries()).thenReturn(objectSummaries());

        List<URI> uris = s3FileInput.expandUri();
        assertThat(uris).hasSize(2);
        assertThat(uris.get(0)).hasToString("s3:///fakeBucket/prefix/test1.json.gz");
        assertThat(uris.get(1)).hasToString("s3:///fakeBucket/prefix/test2.json.gz");
    }

    private List<S3ObjectSummary> objectSummaries() {
        listObjectSummaries = new LinkedList<>();

        S3ObjectSummary firstObj = new S3ObjectSummary();
        S3ObjectSummary secondObj = new S3ObjectSummary();
        firstObj.setBucketName(BUCKET_NAME);
        secondObj.setBucketName(BUCKET_NAME);
        firstObj.setKey("prefix/test1.json.gz");
        secondObj.setKey("prefix/test2.json.gz");
        listObjectSummaries.add(firstObj);
        listObjectSummaries.add(secondObj);
        return listObjectSummaries;
    }

    @Test
    public void test_toPreGlobUri() {
        var uris =
            List.of(
                "s3:///fakeBucket3/prefix*/*.json",
                "s3://fakeBucket3/*/prefix2/prefix3/a.json",
                "s3://fakeBucket3/prefix/p*x/*/*.json",
                "s3://fake.Bucket/prefix/key*",
                "s3://minio:minio@play.min.io:9000/myBucket/myKey/*",
                "s3://play.min.io:9000/myBucket/myKey/*",
                "s3://minio:minio@myBucket/myKey/*"
            );
        var preGlobURIs = uris.stream()
            .map(URI::create)
            .map(u -> Objects.requireNonNull(S3FileInput.toPreGlobUri(S3URI.toS3URI(u))).toString())
            .toList();
        assertThat(preGlobURIs).isEqualTo(List.of(
            "s3:///fakeBucket3/",
            "s3:///fakeBucket3/",
            "s3:///fakeBucket3/prefix/",
            "s3:///fake.Bucket/prefix/",
            "s3://minio:minio@play.min.io:9000/myBucket/myKey/",
            "s3://play.min.io:9000/myBucket/myKey/",
            "s3://minio:minio@/myBucket/myKey/"
        ));
    }
}

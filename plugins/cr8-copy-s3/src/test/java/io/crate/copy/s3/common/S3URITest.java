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

package io.crate.copy.s3.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;

import org.junit.jupiter.api.Test;

class S3URITest {

    private static S3URI parse(String uri) {
        return S3URI.of(URI.create(uri));
    }

    @Test
    public void test_bucket_as_host() {
        assertThat(parse("s3://bucketname_not_host:9000/*/*/a"))
            .isEqualTo(new S3URI("bucketname_not_host:9000", "*/*/a", null, null, null));
        assertThat(parse("s3://b/prefix/"))
            .isEqualTo(new S3URI("b", "prefix", null, null, null));
    }

    @Test
    public void test_bucket_as_host_no_path() throws Exception {
        assertThat(parse("s3://bucket"))
            .isEqualTo(new S3URI("bucket", "", null, null, null));
        assertThat(parse("s3:/bucket"))
            .isEqualTo(new S3URI("bucket", "", null, null, null));
    }

    @Test
    public void test_bucket_in_path_host_as_endpoint() throws Exception {
        assertThat(parse("s3://minio:pw@example.com:9000/mj.myb/somepath:0000/"))
            .isEqualTo(new S3URI("mj.myb", "somepath:0000", "example.com:9000", "minio", "pw"));
        assertThat(parse("s3://minio:pw@example.com:9000/mj.myb/somepath:0000"))
            .isEqualTo(new S3URI("mj.myb", "somepath:0000", "example.com:9000", "minio", "pw"));

        assertThat(parse("s3://h:7/b/p"))
            .isEqualTo(new S3URI("b", "p", "h:7", null, null));
        assertThat(parse("s3://h:7/b/*"))
            .isEqualTo(new S3URI("b", "*", "h:7", null, null));

        assertThat(parse("s3://minio:miniostorage%2F@127.0.0.1:9000/my-bucket/key/t1_0_.json"))
            .isEqualTo(new S3URI("my-bucket", "key/t1_0_.json", "127.0.0.1:9000", "minio", "miniostorage/"));
    }

    @Test
    public void test_user_without_host_and_bucket_in_path() throws Exception {
        assertThat(parse("s3://arthur:pw@/mj.myb/foo/bar/"))
            .isEqualTo(new S3URI("mj.myb", "foo/bar", null, "arthur", "pw"));
    }

    @Test
    public void test_keys_are_decoded() throws Exception {
        assertThat(parse("s3://access%2F:secret%2F@a/b"))
            .isEqualTo(new S3URI("b", "", "a", "access/", "secret/"));
    }

    @Test
    public void test_empty() throws Exception {
        assertThat(parse("s3:///"))
            .isEqualTo(new S3URI("", "", null, null, null));
    }
}

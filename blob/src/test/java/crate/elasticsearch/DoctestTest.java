package crate.elasticsearch;

import crate.test.integration.DoctestTestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.python.core.*;
import org.python.util.PythonInterpreter;

import java.net.URL;


public class DoctestTest extends DoctestTestCase {

    @Test
    public void testBlob() throws Exception {

        createIndex(settingsBuilder()
                .put("blobs.enabled", true)
                .put("number_of_shards", 2)
                .put("number_of_replicas", 0).build(), "test", "test_blobs2");

        createIndex(settingsBuilder()
                .put("blobs.enabled", false)
                .put("number_of_shards", 2)
                .put("number_of_replicas", 0).build(), "test_no_blobs");

        execDocFile("integrationtests/blob.rst");

    }

}

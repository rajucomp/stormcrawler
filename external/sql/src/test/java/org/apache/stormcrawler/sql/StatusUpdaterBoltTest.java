/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.stormcrawler.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.persistence.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class StatusUpdaterBoltTest {

    private static final DockerImageName MYSQL_IMAGE = DockerImageName.parse("mysql:8.4.0");

    @Container
    private static final MySQLContainer<?> mysqlContainer =
            new MySQLContainer<>(MYSQL_IMAGE)
                    .withDatabaseName("crawl")
                    .withUsername("crawler")
                    .withPassword("crawler");

    private Connection testConnection;
    private TestOutputCollector output;

    @BeforeEach
    void setup() throws Exception {
        output = new TestOutputCollector();
        // Create table
        testConnection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword());

        try (Statement stmt = testConnection.createStatement()) {

            stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS urls (
                        url VARCHAR(255),
                        status VARCHAR(16) DEFAULT 'DISCOVERED',
                        nextfetchdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        metadata TEXT,
                        bucket SMALLINT DEFAULT 0,
                        host VARCHAR(128),
                        PRIMARY KEY(url)
                    )
                    """);
        }
    }

    @AfterEach
    void cleanup() throws Exception {
        if (testConnection != null) {
            testConnection.close();
        }
    }

    @Test
    void testStoreDiscoveredURL() throws Exception {
        StatusUpdaterBolt bolt = createBolt();
        String url = "http://example.com/page1";
        Metadata metadata = new Metadata();
        metadata.addValue("key1", "value1");

        Tuple tuple = createTuple(url, Status.DISCOVERED, metadata);
        bolt.execute(tuple);

        // DISCOVERED URLs are batched and the batch executes after 2 seconds (batchMaxIdleMsec)
        // Wait long enough for the batch to be executed
        Thread.sleep(3000);

        // Verify URL was stored
        try (Statement stmt = testConnection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM urls WHERE url = '" + url + "'")) {
            assertTrue(rs.next(), "URL should be stored in database after batch execution");
            assertEquals("DISCOVERED", rs.getString("status"));
            assertNotNull(rs.getString("metadata"));
        }
        bolt.cleanup();
    }

    @Test
    void testUpdateURL() throws Exception {
        StatusUpdaterBolt bolt = createBolt();
        String url = "http://example.com/page2";
        Metadata metadata = new Metadata();
        metadata.addValue("key1", "value1");

        // First store as DISCOVERED
        Tuple tuple1 = createTuple(url, Status.DISCOVERED, metadata);
        bolt.execute(tuple1);

        // Now update to FETCHED
        Tuple tuple2 = createTuple(url, Status.FETCHED, metadata);
        bolt.execute(tuple2);

        // Verify URL was updated
        try (Statement stmt = testConnection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM urls WHERE url = '" + url + "'")) {
            assertTrue(rs.next());
            assertEquals("FETCHED", rs.getString("status"));
            assertNotNull(rs.getString("metadata"));
        }
        bolt.cleanup();
    }

    private Tuple createTuple(String url, Status status, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("status")).thenReturn(status);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        return tuple;
    }

    private Map<String, Object> createTestConfig() {
        Map<String, Object> conf = new HashMap<>();

        Map<String, String> sqlConnection = new HashMap<>();
        sqlConnection.put("url", mysqlContainer.getJdbcUrl());
        sqlConnection.put("user", mysqlContainer.getUsername());
        sqlConnection.put("password", mysqlContainer.getPassword());
        conf.put("sql.connection", sqlConnection);

        conf.put("sql.status.table", "urls");
        conf.put("sql.status.max.urls.per.bucket", 10);
        conf.put("scheduler.class", "org.apache.stormcrawler.persistence.DefaultScheduler");
        // Add cache configuration to prevent NullPointerException
        conf.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");

        return conf;
    }

    private StatusUpdaterBolt createBolt() {
        StatusUpdaterBolt bolt = new StatusUpdaterBolt();
        Map<String, Object> conf = createTestConfig();
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        return bolt;
    }
}

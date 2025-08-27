/**
 * Copyright (c) 2013 - 2025 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.Record;
import org.neo4j.driver.*;
import site.ycsb.*;

/**
 * A class that wraps neo4j to allow it to be interfaced
 * with YCSB. This class extends {@link DB} and implements neo4j.
 *
 * <br>
 * Each client will have its own instance of this class. This client
 * may or may not be thread safe.
 *
 * <br>
 * This interface works without any schema, but it is heavily advised to
 * create an index on the _key property. Consult the README for more information
 */
public class Neo4jClient extends DB {
  private Driver driver;

  /** Integer used to keep track of current threads. */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /**
   * Initializes the graph database, only once per DB instance.
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    synchronized (Neo4jClient.class) {
      if (driver == null) {
        Properties properties = getProperties();
        String url = properties.getProperty("neo4j.url");
        try {
          driver = GraphDatabase.driver(url,
              AuthTokens.basic(properties.getProperty("neo4j.user"), properties.getProperty("neo4j.passwd")));
          driver.verifyConnectivity();
          System.out.println("neo4j client connection created with " + url);
        } catch (Exception e) {
          System.err.println("Could not initialize connection to neo4j: " + e.toString());
          e.printStackTrace();
        }
      }

    }
  }

  /**
   * Shuts down the Neo4j graph database, called once per DB instance.
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      driver.close();
    }
  }

  /**
   * Reads a set of fields found in a labelled node.
   *
   * @param table  Table name (acts as a label)
   * @param key    Record key of the node to read
   * @param fields Fields to read
   * @param result A HashMap with field/value pairs
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      EagerResult res = driver.executableQuery("MATCH (n:" + table + " {_key: $key}) RETURN n")
          .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
          .withParameters(Map.of("key", key))
          .execute();

      Map<String, Object> record = res.records().get(0).asMap();

      if (fields != null) {
        for (String field : fields) {
          result.put(field, new StringByteIterator((String) record.get(field)));
        }
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table       Table name (acts as a label)
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      EagerResult res = driver
          .executableQuery("MATCH (n:" + table + ") WHERE n._key >= $startkey RETURN n LIMIT $recordCount")
          .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
          .withParameters(Map.of("label", table, "startkey", startkey, "recordCount", recordcount))
          .execute();

      List<Record> records = res.records();

      if (records != null) {
        for (Record record : records) {
          HashMap<String, ByteIterator> nodeScanResult = new HashMap<>();

          if (fields != null) {
            for (String field : fields) {
              nodeScanResult.put(field, new StringByteIterator((String) record.asMap().get(field)));
            }
          }

          result.add(nodeScanResult);
        }
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Updates a new node in the neo4j database. Any field/value pairs in the specified
   * values HashMap will be written into the node with the specified node
   * key, overwriting any existing values with the same property name.
   *
   * @param table       Table name (acts as a label)
   * @param key         Record key of node to update
   * @param values      Values to insert/update (field-value hashmap)
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      Map<String, Object> props = new HashMap<>();

      // the ByteIterator type needs to be converted to an Object
      // because it is not supported by the neo4j driver
      for (Entry entry : values.entrySet()) {
        props.put(entry.getKey().toString(), entry.getValue().toString());
      }

      props.put("_key", key); // SET wipes all previous values so the key needs to be included in the values parameter

      driver.executableQuery("MATCH (n:" + table + " {_key: $key}) SET n = $values")
          .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
          .withParameters(Map.of("key", key, "values", props)).execute();

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Inserts a new node in the neo4j database. Any field/value pairs in the specified
   * values HashMap will be written into the node with the specified node
   * key.
   *
   * @param table  Table name (acts as a label)
   * @param key    Record key of created node
   * @param values Values to insert (key-value hashmap)
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      Map<String, Object> props = new HashMap<>();

      // the ByteIterator type needs to be converted to an Object
      // because it is not supported by the neo4j driver
      for (Entry entry : values.entrySet()) {
        props.put(entry.getKey().toString(), entry.getValue().toString());
      }

      props.put("_key", key); // SET wipes all previous values so the key needs to be included in the values parameter

      driver.executableQuery("CREATE (n:" + table + ") SET n = $values")
          .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
          .withParameters(Map.of("key", key, "values", props)).execute();

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Deletes a node found by its label and property.
   *
   * @param table  Table name (acts as a label)
   * @param key    Record key of node to delete
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {
    try {
      driver.executableQuery("MATCH (n:" + table + " {_key: $key}) DELETE n")
          .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
          .withParameters(Map.of("key", key)).execute();

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }
}
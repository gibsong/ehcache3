/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehcache;

import java.io.File;
import java.io.IOException;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.junit.Test;

import java.util.Collections;
import org.ehcache.config.units.MemoryUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.terracotta.connection.Connection;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

/**
 * @author Ludovic Orban
 */
public class Clust {

  private static final String RESOURCE_CONFIG =
      "<service xmlns:ohr='http://www.terracotta.org/config/offheap-resource' id=\"resources\">"
          + "<ohr:offheap-resources>"
          + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
          + "</ohr:offheap-resources>" +
          "</service>\n";

  @ClassRule
  public static Cluster CLUSTER = new BasicExternalCluster(new File("build/cluster"), 1, Collections.<File>emptyList(), "", RESOURCE_CONFIG, "");
  private static Connection CONNECTION;

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    CONNECTION = CLUSTER.newConnection();
  }

  @AfterClass
  public static void closeConnection() throws IOException {
    CONNECTION.close();
  }

  @Test
  public void works() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("clustered-cache-works", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 16, MemoryUnit.MB))
            )
//            .add(new ClusteredStoreConfiguration(Consistency.STRONG, 1024))
        )
        .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/cache-manager-works"))
            .autoCreate()
//            .defaultServerResource("primary-server-resource")
//            .resourcePool("resource-pool-a", 28, MemoryUnit.MB)
        );

    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    cacheManager.close();
  }

}

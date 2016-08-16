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
package org.ehcache.jsr107;

import static java.util.Arrays.asList;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.jsr107.Eh107Configuration.fromEhcacheCacheConfiguration;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import javax.cache.Cache;
import javax.cache.CacheManager;
import org.ehcache.config.Builder;
import org.ehcache.config.Configuration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.DefaultConfiguration;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class Eh107CacheStatisticsMXBeanTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {

    int numEntries = 10;
    int expectedEvictions = 1;
    int numValueChars = 1000000;
    int iterations = 2;

    return asList(new Object[][] {
      { newResourcePoolsBuilder().heap(numEntries, ENTRIES), numEntries+1, expectedEvictions, numValueChars},
      { newResourcePoolsBuilder().offheap(1, MemoryUnit.MB), iterations, expectedEvictions, numValueChars},
      //TODO standalone disk test fails for numValueChars = 1000000,
      //Questions:
      //1. does disk use UTF-16?  or something that requires 2 bytes per char? I think this is the problem.  or
      //2. does meta data take up space?
      //{ newResourcePoolsBuilder().disk(1, MemoryUnit.MB), iterations, expectedEvictions, numValueChars},

      //TODO need clustered standalone test
      //{ newResourcePoolsBuilder().clustered(1, MemoryUnit.MB), iterations, expectedEvictions, numValueChars},

      //1 heap eviction, 2 off heap evictions.
      { newResourcePoolsBuilder().heap(2, ENTRIES).offheap(1, MemoryUnit.MB), 3, 2, numValueChars},

      //TODO test remaining 2 tier combinations.  Sort out disk issue.
      //heap, disk
      //{ newResourcePoolsBuilder().heap(2, ENTRIES).disk(1, MemoryUnit.MB), 3, 2, numValueChars},  //1 heap eviction, 2 disk evictions.
      //offheap, disk
      //offheap, clustered
      //disk, clustered

      //TODO figure out disk issue and finish this test
      //1 heap eviction, 1 offheap eviction, 2 disk evictions.
      //{ newResourcePoolsBuilder().heap(2, ENTRIES).offheap(2, MemoryUnit.MB).disk(3, MemoryUnit.MB), 4, 1, numValueChars},
    });
  }

  private final ResourcePools resources;
  private final int iterations;
  private final long expected;
  private final int valueByteSize;

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  public Eh107CacheStatisticsMXBeanTest(Builder<? extends ResourcePools> resources, int iterations, long expected, int valueByteSize) {
    this.resources = resources.build();
    this.iterations = iterations;
    this.expected = expected;
    this.valueByteSize = valueByteSize;
  }

  @Test
  public void findLowestTierStatisticTest() throws IOException {
    char[] value = new char[valueByteSize];
    Arrays.fill(value, 'x');

    Configuration config = new DefaultConfiguration(Eh107CacheStatisticsMXBeanTest.class.getClassLoader(),
            new DefaultPersistenceConfiguration(diskPath.newFolder()));
    CacheManager cacheManager = new EhcacheCachingProvider().getCacheManager(URI.create("myCacheMgr"), config);

    try {
      Cache<Long, String> cache = cacheManager.createCache("cacheAlias", fromEhcacheCacheConfiguration(
                      newCacheConfigurationBuilder(Long.class, String.class, resources)));

      for(long i=0; i<iterations; i++) {
        cache.put(i, new String(value));
      }

      Eh107CacheStatisticsMXBean heapStatistics = (Eh107CacheStatisticsMXBean) ((Eh107Cache<Long, String>) cache).getStatisticsMBean();
      Assert.assertThat(heapStatistics.getCacheEvictions(),is(expected));

    } finally {
      cacheManager.close();
    }
  }

}

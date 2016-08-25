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
package org.ehcache.management.providers.statistics;

import java.util.Arrays;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.history.CounterHistory;
import org.terracotta.management.model.stats.history.RatioHistory;

/**
 *
 *
 */
public class StandardEhcacheStatisticsTest {

  @Test
  public void statsCacheMissTest() throws InterruptedException {
    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager");
    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(
        Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder().heap(1, MemoryUnit.MB).offheap(10, MemoryUnit.MB))
        .build();

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true)) {

      Cache<Long, String> cache = cacheManager.getCache("aCache", Long.class, String.class);
      cache.put(1L, "one");

      cache.get(1L);//HIT
      cache.get(1L);//HIT
      cache.get(2L);//MISS
      cache.get(3L);//MISS

      Thread.sleep(1000);

      Context context = StatsUtil.createContext(managementRegistry);

      ContextualStatistics missCounter = managementRegistry.withCapability("StatisticsCapability")
          .queryStatistics(Arrays.asList(/*"Cache:MissRate",*/ "Cache:MissCount", "Cache:MissRatio"))//TODO add Cache:MissRate once understand how to verify correct
          .on(context)
          .build()
          .execute()
          .getSingleResult();

      Assert.assertThat(missCounter.size(), Matchers.is(2));

      /*RateHistory missRateHistory = missCounter.getStatistic(RateHistory.class, "Cache:MissRate");
      while(!isHistoryReady(missRateHistory, 0d)) {}
      //TODO how can i calculate rate? miss/second
      Assert.assertThat(missRateHistory.getValue()[0].getValue(), Matchers.greaterThan(0d));*/

      CounterHistory missCountCounterHistory = missCounter.getStatistic(CounterHistory.class, "Cache:MissCount");
      while(!StatsUtil.isHistoryReady(missCountCounterHistory, 0L)) {}
      Assert.assertThat(missCountCounterHistory.getValue()[0].getValue(), Matchers.equalTo(2L));

      RatioHistory ratioHistory = missCounter.getStatistic(RatioHistory.class, "Cache:MissRatio");
      while(!StatsUtil.isHistoryReady(ratioHistory, Double.POSITIVE_INFINITY)) {}
      Assert.assertThat(ratioHistory.getValue()[0].getValue(), Matchers.equalTo(1d));
    }
  }

  @Test
  public void statsCacheHitTest() throws InterruptedException {
    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager");
      ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(1, MemoryUnit.MB).offheap(10, MemoryUnit.MB))
        .build();

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache("aCache", cacheConfiguration)
          .using(managementRegistry)
          .build(true)) {

      Cache<Long, String> cache = cacheManager.getCache("aCache", Long.class, String.class);
      cache.put(1L, "1");
      cache.put(2L, "2");
      cache.put(3L, "3");

      cache.get(1L);//HIT
      cache.get(2L);//HIT
      cache.get(2L);//HIT
      cache.get(4L);//need a MISS for ratio, otherwise you get infinity as a value

      Thread.sleep(1000);

      Context context = StatsUtil.createContext(managementRegistry);

      ContextualStatistics contextualStatistics = managementRegistry.withCapability("StatisticsCapability")
          .queryStatistics(Arrays.asList(/*"Cache:HitRate",*/ "Cache:HitCount", "Cache:HitRatio"))//TODO add Cache:HitRate once understand how to verify correct
          .on(context)
          .build()
          .execute()
          .getSingleResult();

      Assert.assertThat(contextualStatistics.size(), Matchers.is(2));

      /*RateHistory hitRateHistory = hitCounter.getStatistic(RateHistory.class, "Cache:HitRate");
      while(!isHistoryReady(hitRateHistory, 0d)) {}
      //TODO how can i calculate rate? hits/second
      Assert.assertThat(hitRateHistory.getValue()[0].getValue(), Matchers.greaterThan(0d));*/

      CounterHistory hitCountCounterHistory = contextualStatistics.getStatistic(CounterHistory.class, "Cache:HitCount");
      while(!StatsUtil.isHistoryReady(hitCountCounterHistory, 0L)) {}
      Assert.assertThat(hitCountCounterHistory.getValue()[0].getValue(), Matchers.equalTo(3L));

      RatioHistory ratioHistory = contextualStatistics.getStatistic(RatioHistory.class, "Cache:HitRatio");
      while(!StatsUtil.isHistoryReady(ratioHistory, Double.POSITIVE_INFINITY)) {}
      Assert.assertThat(ratioHistory.getValue()[0].getValue(), Matchers.equalTo(3d));
    }
  }

  @Test
  public void statsClearCacheTest() throws InterruptedException {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(1, EntryUnit.ENTRIES).offheap(10, MemoryUnit.MB))
        .build();

    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager");
    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Cache<Long, String> aCache = cacheManager.getCache("aCache", Long.class, String.class);
    aCache.put(1L, "one");
    Assert.assertTrue(aCache.containsKey(1L));
    aCache.clear();
    Assert.assertFalse(aCache.iterator().hasNext());

    aCache.put(1L, "one");
    Assert.assertTrue(aCache.containsKey(1L));
    aCache.clear();
    Assert.assertFalse(aCache.iterator().hasNext());

    Thread.sleep(1000);

    Context context = StatsUtil.createContext(managementRegistry);

    ContextualStatistics clearCounter = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistics(Arrays.asList("Cache:ClearCount"))
        .on(context)
        .build()
        .execute()
        .getSingleResult();

    Assert.assertThat(clearCounter.size(), Matchers.is(1));
    CounterHistory cache_Clear_Count = clearCounter.getStatistic(CounterHistory.class, "Cache:ClearCount");

    while(!StatsUtil.isHistoryReady(cache_Clear_Count, 0)) {}
    Assert.assertThat(cache_Clear_Count.getValue()[0].getValue(), Matchers.equalTo(2L));

    cacheManager.close();
  }
}

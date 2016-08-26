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
package org.ehcache.management.registry;

import java.io.File;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.management.ManagementRegistryService;
import org.terracotta.management.registry.ResultSet;
import org.terracotta.management.registry.StatisticQuery;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.terracotta.management.model.call.ContextualReturn;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.Sample;
import org.terracotta.management.model.stats.history.CounterHistory;


import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.ResourcePoolsBuilder;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.registry.StatisticQuery.Builder;

public class DefaultManagementRegistryServiceTest {

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  @Test
  public void testCanGetContext() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    assertThat(managementRegistry.getContextContainer().getName(), equalTo("cacheManagerName"));
    assertThat(managementRegistry.getContextContainer().getValue(), equalTo("myCM"));
    assertThat(managementRegistry.getContextContainer().getSubContexts(), hasSize(1));
    assertThat(managementRegistry.getContextContainer().getSubContexts().iterator().next().getName(), equalTo("cacheName"));
    assertThat(managementRegistry.getContextContainer().getSubContexts().iterator().next().getValue(), equalTo("aCache"));

    cacheManager1.close();
  }

  @Test
  public void descriptorOnHeapTest()
  {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    assertThat(managementRegistry.getCapabilities(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getName(), equalTo("ActionsCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getName(), equalTo("StatisticsCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(2).getName(), equalTo("StatisticCollectorCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getName(), equalTo("SettingsCapability"));

    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getDescriptors(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getDescriptors(), hasSize(37));

    cacheManager1.close();
  }

  @Test
  public void descriptorOffHeapTest()
  {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().heap(5, MB).offheap(10, MB))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    assertThat(managementRegistry.getCapabilities(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getName(), equalTo("ActionsCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getName(), equalTo("StatisticsCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(2).getName(), equalTo("StatisticCollectorCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getName(), equalTo("SettingsCapability"));

    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getDescriptors(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getDescriptors(), hasSize(57));

    cacheManager1.close();
  }

  @Test
  public void descriptorDiskStoreTest() throws URISyntaxException
  {
    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(CacheManagerBuilder.persistence(getStoragePath() + File.separator + "myData"))
        .withCache("persistent-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .disk(10, MemoryUnit.MB, true))
            )
        .using(managementRegistry)
        .build(true);

    assertThat(managementRegistry.getCapabilities(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getName(), equalTo("ActionsCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getName(), equalTo("StatisticsCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(2).getName(), equalTo("StatisticCollectorCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getName(), equalTo("SettingsCapability"));

    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getDescriptors(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getDescriptors(), hasSize(57));

    persistentCacheManager.close();
  }

  private String getStoragePath() throws URISyntaxException {
    return getClass().getClassLoader().getResource(".").toURI().getPath();
  }


  @Test
  public void testCanGetCapabilities() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    assertThat(managementRegistry.getCapabilities(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getName(), equalTo("ActionsCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getName(), equalTo("StatisticsCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(2).getName(), equalTo("StatisticCollectorCapability"));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(3).getName(), equalTo("SettingsCapability"));

    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getDescriptors(), hasSize(4));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getDescriptors(), hasSize(37));

    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(0).getCapabilityContext().getAttributes(), hasSize(2));
    assertThat(new ArrayList<Capability>(managementRegistry.getCapabilities()).get(1).getCapabilityContext().getAttributes(), hasSize(2));

    cacheManager1.close();
  }

  @Test (timeout = 5000)
  public void testCanGetStats() {
    String queryStatisticName = "Cache:HitCount";

    long averageWindowDuration = 1;
    TimeUnit averageWindowUnit = TimeUnit.MINUTES;
    int historySize = 100;
    long historyInterval = 1;
    TimeUnit historyIntervalUnit = TimeUnit.MILLISECONDS;
    long timeToDisable = 10;
    TimeUnit timeToDisableUnit = TimeUnit.MINUTES;
    EhcacheStatisticsProviderConfiguration config = new EhcacheStatisticsProviderConfiguration(averageWindowDuration,averageWindowUnit,historySize,historyInterval,historyIntervalUnit,timeToDisable,timeToDisableUnit);

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM").addConfiguration(config));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .withCache("aCache2", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Context context1 = Context.empty()
      .with("cacheManagerName", "myCM")
      .with("cacheName", "aCache1");

    Context context2 = Context.empty()
      .with("cacheManagerName", "myCM")
      .with("cacheName", "aCache2");

    Cache cache1 = cacheManager1.getCache("aCache1", Long.class, String.class);
    Cache cache2 = cacheManager1.getCache("aCache2", Long.class, String.class);

    cache1.put(1L, "one");
    cache2.put(3L, "three");

    cache1.get(1L);
    cache1.get(2L);
    cache2.get(3L);
    cache2.get(4L);

    Builder builder1 = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistic(queryStatisticName)
        .on(context1);

    ContextualStatistics counters = getResultSet(builder1, context1, null, CounterHistory.class, queryStatisticName).getResult(context1);
    CounterHistory counterHistory1 = counters.getStatistic(CounterHistory.class, queryStatisticName);

    assertThat(counters.size(), equalTo(1));
    int mostRecentSampleIndex = counterHistory1.getValue().length - 1;
    assertThat(counterHistory1.getValue()[mostRecentSampleIndex].getValue(), equalTo(1L));

    Builder builder2 = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistic(queryStatisticName)
        .on(context1)
        .on(context2);
    ResultSet<ContextualStatistics> allCounters = getResultSet(builder2, context1, context2, CounterHistory.class, queryStatisticName);

    assertThat(allCounters.size(), equalTo(2));
    assertThat(allCounters.getResult(context1).size(), equalTo(1));
    assertThat(allCounters.getResult(context2).size(), equalTo(1));

    mostRecentSampleIndex = allCounters.getResult(context1).getStatistic(CounterHistory.class, queryStatisticName).getValue().length - 1;
    assertThat(allCounters.getResult(context1).getStatistic(CounterHistory.class, queryStatisticName).getValue()[mostRecentSampleIndex].getValue(), equalTo(1L));

    mostRecentSampleIndex = allCounters.getResult(context2).getStatistic(CounterHistory.class, queryStatisticName).getValue().length - 1;
    assertThat(allCounters.getResult(context2).getStatistic(CounterHistory.class, queryStatisticName).getValue()[mostRecentSampleIndex].getValue(), equalTo(1L));

    cacheManager1.close();
  }

  private static ResultSet<ContextualStatistics> getResultSet(Builder builder, Context context1, Context context2, Class<CounterHistory> type, String statisticsName)
  {
    ResultSet<ContextualStatistics> counters;

    while(true)  //wait till Counter history(s) is initialized and contains values.
    {
      counters = builder.build().execute();

      ContextualStatistics statisticsContext1 = counters.getResult(context1);
      CounterHistory counterHistoryContext1 = statisticsContext1.getStatistic(type, statisticsName);

      if(context2 != null)
      {
        ContextualStatistics statisticsContext2 = counters.getResult(context2);
        CounterHistory counterHistoryContext2 = statisticsContext2.getStatistic(type, statisticsName);

        if(counterHistoryContext2.getValue().length > 0 &&
           counterHistoryContext2.getValue()[counterHistoryContext2.getValue().length - 1].getValue() > 0 &&
           counterHistoryContext1.getValue().length > 0 &&
           counterHistoryContext1.getValue()[counterHistoryContext1.getValue().length - 1].getValue() > 0)
        {
          break;
        }
      }
      else
      {
        if(counterHistoryContext1.getValue().length > 0 &&
           counterHistoryContext1.getValue()[counterHistoryContext1.getValue().length - 1].getValue() > 0)
        {
          break;
        }
      }
    }

    return counters;
  }

  @Test (timeout=5000)
  public void testCanGetStatsSinceTime() throws InterruptedException {

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration()
        .addConfiguration(new EhcacheStatisticsProviderConfiguration(5000, TimeUnit.MILLISECONDS, 100, 1, TimeUnit.SECONDS, 30, TimeUnit.SECONDS))
        .setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Context context = Context.empty()
      .with("cacheManagerName", "myCM")
      .with("cacheName", "aCache1");

    StatisticQuery.Builder builder = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistic("Cache:MissCount")
        .on(context);

    ContextualStatistics statistics;
    CounterHistory getCount;
    long timestamp;

    // ------
    // first call to trigger stat collection within ehcache stat framework
    // ------

    builder.build().execute();

    // ------
    // 3 gets and we wait more than 1 second (history frequency) to be sure the scheduler thread has computed a new stat in the history
    // ------

    cacheManager1.getCache("aCache1", Long.class, String.class).get(1L);
    cacheManager1.getCache("aCache1", Long.class, String.class).get(2L);
    cacheManager1.getCache("aCache1", Long.class, String.class).get(2L);

    do {
      Thread.sleep(100);
      statistics = builder.build().execute().getResult(context);
      getCount = statistics.getStatistic(CounterHistory.class);
    } while (getCount.getValue().length < 1);

    // within 1 second of history there has been 3 gets
    assertThat(getCount.getValue()[0].getValue(), equalTo(3L));

    // keep time for next call (since)
    timestamp = getCount.getValue()[0].getTimestamp();

    // ------
    // 2 gets and we wait more than 1 second (history frequency) to be sure the scheduler thread has computed a new stat in the history
    // We will get only the stats SINCE last time
    // ------

    cacheManager1.getCache("aCache1", Long.class, String.class).get(1L);
    cacheManager1.getCache("aCache1", Long.class, String.class).get(2L);

    // ------
    // WITHOUT using since: the history will have 2 values
    // ------

    do {
      Thread.sleep(100);
      statistics = builder.build().execute().getResult(context);
      getCount = statistics.getStatistic(CounterHistory.class);
    } while (getCount.getValue().length < 2);

    // ------
    // WITH since: the history will have 1 value
    // ------

    statistics = builder.since(timestamp + 1).build().execute().getResult(context);
    getCount = statistics.getStatistic(CounterHistory.class);

    // get the counter for each computation at each 1 second
    assertThat(Arrays.asList(getCount.getValue()), everyItem(Matchers.<Sample<Long>>hasProperty("timestamp", greaterThan(timestamp))));

    cacheManager1.close();
  }

  @Test
  public void testCall() throws ExecutionException {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .withCache("aCache2", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Context context = Context.empty()
      .with("cacheManagerName", "myCM")
      .with("cacheName", "aCache1");

    cacheManager1.getCache("aCache1", Long.class, String.class).put(1L, "1");

    assertThat(cacheManager1.getCache("aCache1", Long.class, String.class).get(1L), equalTo("1"));

    ContextualReturn<?> result = managementRegistry.withCapability("ActionsCapability")
        .call("clear")
        .on(context)
        .build()
        .execute()
        .getSingleResult();

    assertThat(result.hasExecuted(), is(true));
    assertThat(result.getValue(), is(nullValue()));

    assertThat(cacheManager1.getCache("aCache1", Long.class, String.class).get(1L), is(Matchers.nullValue()));

    cacheManager1.close();
  }

  @Test
  public void testCallOnInexistignContext() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM"));

    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .withCache("aCache2", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Context inexisting = Context.empty()
        .with("cacheManagerName", "myCM2")
        .with("cacheName", "aCache2");

    ResultSet<? extends ContextualReturn<?>> results = managementRegistry.withCapability("ActionsCapability")
        .call("clear")
        .on(inexisting)
        .build()
        .execute();

    assertThat(results.size(), equalTo(1));
    assertThat(results.getSingleResult().hasExecuted(), is(false));

    try {
      results.getSingleResult().getValue();
      fail();
    } catch (Exception e) {
      assertThat(e, instanceOf(NoSuchElementException.class));
    }

    cacheManager1.close();
  }

}

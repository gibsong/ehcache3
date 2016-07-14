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
package org.ehcache.core.statistics;

import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.context.annotations.ContextAttribute;
import org.terracotta.context.query.Matcher;
import org.terracotta.context.query.Matchers;
import org.terracotta.context.query.Query;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.ValueStatistic;
import org.terracotta.statistics.observer.ChainedOperationObserver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;
import static org.terracotta.context.query.Matchers.identifier;
import static org.terracotta.context.query.Matchers.subclassOf;
import static org.terracotta.context.query.Queries.self;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;

/**
 * @author Ludovic Orban
 */
@ContextAttribute("this")
public class TierOperationStatistic<S extends Enum<S>, D extends Enum<D>> implements OperationStatistic<D> {

  @ContextAttribute("name") public final String name;
  @ContextAttribute("tags") public final Set<String> tags;
  @ContextAttribute("properties") public final Map<String, Object> properties;
  @ContextAttribute("type") public final Class<D> type;

  private final Class<D> aliasing;
  private final OperationStatistic<S> shadow;
  private final HashMap<D, Set<S>> xlatMap;

  public TierOperationStatistic(Class<D> aliasing, Class<S> aliased, OperationStatistic<S> shadow, HashMap<D, Set<S>> xlatMap, String name, int priority, String discriminator) {
    this.aliasing = aliasing;
    this.shadow = shadow;
    this.xlatMap = xlatMap;
    this.name = name;
    this.tags = new HashSet<String>();
    this.tags.add("tier");
    this.properties = new HashMap<String, Object>();
    this.properties.put("priority", priority);
    if (discriminator != null) {
      this.properties.put("discriminator", discriminator);
    }
    this.type = aliasing;

    EnumSet<D> ds = EnumSet.allOf(aliasing);
    for (D d : ds) {
      if (!xlatMap.containsKey(d)) {
        throw new IllegalArgumentException("xlatMap does not contain key " + d);
      }
    }

    Set<S> allAliasedValues = new HashSet<S>();
    Collection<Set<S>> values = xlatMap.values();
    for (Set<S> value : values) {
      allAliasedValues.addAll(value);
    }
    Set<S> allMissingValues = new HashSet<S>(EnumSet.allOf(aliased));
    allMissingValues.removeAll(allAliasedValues);
    if (!allMissingValues.isEmpty()) {
      throw new IllegalArgumentException("xlatMap does not contain values " + allMissingValues);
    }
  }

  @Override
  public Class<D> type() {
    return aliasing;
  }

  @Override
  public ValueStatistic<Long> statistic(D result) {
    return shadow.statistic(xlatMap.get(result));
  }

  @Override
  public ValueStatistic<Long> statistic(Set<D> results) {
    Set<S> xlated = new HashSet<S>();
    for (D result : results) {
      xlated.addAll(xlatMap.get(result));
    }
    return shadow.statistic(xlated);
  }

  @Override
  public long count(D type) {
    long value = 0L;
    Set<S> s = xlatMap.get(type);
    for (S s1 : s) {
      value += shadow.count(s1);
    }
    return value;
  }

  @Override
  public long sum(Set<D> types) {
    Set<S> xlated = new HashSet<S>();
    for (D type : types) {
      xlated.addAll(xlatMap.get(type));
    }
    return shadow.sum(xlated);
  }

  @Override
  public long sum() {
    return shadow.sum();
  }

  @Override
  public void addDerivedStatistic(final ChainedOperationObserver<? super D> derived) {
    shadow.addDerivedStatistic(new ChainedOperationObserver<S>() {
      @Override
      public void begin(long time) {
        derived.begin(time);
      }

      @Override
      public void end(long time, S result) {
        derived.end(time, (D) result);
      }

      @Override
      public void end(long time, S result, long... parameters) {
        derived.end(time, (D) result, parameters);
      }
    });
  }

  @Override
  public void removeDerivedStatistic(ChainedOperationObserver<? super D> derived) {
    shadow.removeDerivedStatistic((ChainedOperationObserver<? super S>) derived);
  }

  @Override
  public void begin() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void end(D result) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void end(D result, long... parameters) {
    throw new UnsupportedOperationException();
  }

  public static String findDiscriminator(Object rootNode) {
    Set<TreeNode> results = queryBuilder().chain(self())
        .children().filter(
            context(attributes(Matchers.allOf(
                hasAttribute("discriminator", new Matcher<Object>() {
                  @Override
                  protected boolean matchesSafely(Object object) {
                    return object instanceof String;
                  }
                }))))).build().execute(Collections.singleton(ContextManager.nodeFor(rootNode)));

    if (results.size() > 1) {
      throw new IllegalStateException("More than one discriminator attribute found");
    } else if (results.isEmpty()) {
      return null;
    } else {
      TreeNode node = results.iterator().next();
      return (String) node.getContext().attributes().get("discriminator");
    }
  }

  public static OperationStatistic findOperationStat(Object rootNode, final String statName) {
    Query q = queryBuilder().chain(self())
        .descendants().filter(context(identifier(subclassOf(OperationStatistic.class)))).build();

    Set<TreeNode> operationStatisticNodes = q.execute(Collections.singleton(ContextManager.nodeFor(rootNode)));
    Set<TreeNode> result = queryBuilder()
        .filter(
            context(attributes(Matchers.<Map<String, Object>>allOf(
                hasAttribute("name", statName))))).build().execute(operationStatisticNodes);

    if (result.size() != 1) {
      throw new RuntimeException("a single stat was expected; found " + result.size());
    }

    TreeNode node = result.iterator().next();
    return (OperationStatistic) node.getContext().attributes().get("this");
  }

  public static SortedMap<Integer, OperationStatistic> findTiers(Object rootNode) {
    Query q = queryBuilder().chain(self())
        .descendants().filter(context(identifier(subclassOf(OperationStatistic.class)))).build();

    Set<TreeNode> operationStatisticNodes = q.execute(Collections.singleton(ContextManager.nodeFor(rootNode)));
    Set<TreeNode> result = queryBuilder()
        .filter(
            context(attributes(Matchers.allOf(
                hasAttribute("tags", new Matcher<Set<String>>() {
                  @Override
                  protected boolean matchesSafely(Set<String> object) {
                    return object.contains("tier");
                  }
                }))))).build().execute(operationStatisticNodes);


    SortedMap<Integer, OperationStatistic> rc = new TreeMap<Integer, OperationStatistic>();
    for (TreeNode node : result) {
      OperationStatistic opStat = (OperationStatistic) node.getContext().attributes().get("this");
      Map<String, Object> properties = (Map<String, Object>) node.getContext().attributes().get("properties");
      Integer priority = (Integer) properties.get("priority");
      rc.put(priority, opStat);
    }
    return rc;
  }

  public static boolean existsOperationStat(Object rootNode, final String tag) {
    Query q = queryBuilder().chain(self())
        .descendants().filter(context(identifier(subclassOf(OperationStatistic.class)))).build();

    Set<TreeNode> operationStatisticNodes = q.execute(Collections.singleton(ContextManager.nodeFor(rootNode)));
    Set<TreeNode> result = queryBuilder()
        .filter(
            context(attributes(Matchers.allOf(
                hasAttribute("tags", new Matcher<Set<String>>() {
                  @Override
                  protected boolean matchesSafely(Set<String> object) {
                    return object.contains(tag);
                  }
                }))))).build().execute(operationStatisticNodes);

    return !result.isEmpty();
  }


  public static <X> Set<X> set(X... xs) {
    return new HashSet<X>(Arrays.asList(xs));
  }

  public static class TierResults {

    public enum GetResult {
      HIT,
      MISS,
    }

  }


}

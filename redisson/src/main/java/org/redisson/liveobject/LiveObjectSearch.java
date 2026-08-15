/**
 * Copyright (c) 2013-2026 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.liveobject;

import org.redisson.RedissonObject;
import org.redisson.RedissonScoredSortedSet;
import org.redisson.RedissonSet;
import org.redisson.RedissonSetMultimap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RSet;
import org.redisson.api.RSetMultimap;
import org.redisson.api.condition.Condition;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.liveobject.condition.*;
import org.redisson.liveobject.misc.ClassUtils;
import org.redisson.liveobject.resolver.NamingScheme;

import java.lang.reflect.Field;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiFunction;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class LiveObjectSearch {
    
    private final CommandAsyncExecutor commandExecutor;

    public LiveObjectSearch(CommandAsyncExecutor commandExecutor) {
        this.commandExecutor = commandExecutor;
    }

    private boolean isCollectionField(Class<?> entityClass, String fieldName) {
        try {
            Field field = ClassUtils.getDeclaredField(entityClass, fieldName);
            return Collection.class.isAssignableFrom(field.getType()) || field.getType().isArray();
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private Set<Object> collectionRangeFind(Class<?> entityClass, String fieldName, Number threshold, String operator) {
        NamingScheme namingScheme = commandExecutor.getObjectBuilder().getNamingScheme(entityClass);
        String indexName = namingScheme.getIndexName(entityClass, fieldName);
        RSetMultimap<Object, Object> map = new RedissonSetMultimap<>(namingScheme.getCodec(), commandExecutor, indexName);
        Set<Object> ids = new HashSet<>();
        for (Object key : map.keySet()) {
            if (key instanceof Number) {
                double k = ((Number) key).doubleValue();
                double v = threshold.doubleValue();
                boolean matches = false;
                if ("gt".equals(operator)) {
                    matches = k > v;
                } else if ("ge".equals(operator)) {
                    matches = k >= v;
                } else if ("lt".equals(operator)) {
                    matches = k < v;
                } else if ("le".equals(operator)) {
                    matches = k <= v;
                }
                if (matches) {
                    ids.addAll(map.get(key));
                }
            }
        }
        return ids;
    }

    private boolean handleRangeAnd(Set<Object> allIds, Class<?> entityClass, String fieldName, Number value,
                                     String operator, Map<RScoredSortedSet<Object>, Number> numericMap,
                                     NamingScheme ns) {
        if (isCollectionField(entityClass, fieldName)) {
            Set<Object> ids = collectionRangeFind(entityClass, fieldName, value, operator);
            if (allIds.isEmpty()) {
                allIds.addAll(ids);
            } else {
                allIds.retainAll(ids);
            }
            return !allIds.isEmpty();
        }
        String indexName = ns.getIndexName(entityClass, fieldName);
        numericMap.put(new RedissonScoredSortedSet<>(ns.getCodec(), commandExecutor, indexName, null), value);
        return true;
    }

    private void handleRangeOr(Set<Object> allIds, Class<?> entityClass, String fieldName, Number value,
                                String operator, Map<RScoredSortedSet<Object>, Number> numericMap,
                                NamingScheme ns) {
        if (isCollectionField(entityClass, fieldName)) {
            allIds.addAll(collectionRangeFind(entityClass, fieldName, value, operator));
            return;
        }
        String indexName = ns.getIndexName(entityClass, fieldName);
        numericMap.put(new RedissonScoredSortedSet<>(ns.getCodec(), commandExecutor, indexName, null), value);
    }

    private Set<Object> traverseAnd(ANDCondition condition, NamingScheme namingScheme, Class<?> entityClass) {
        Set<Object> allIds = new HashSet<Object>();
        
        List<String> eqNames = new ArrayList<String>();
        
        Map<RScoredSortedSet<Object>, Number> gtNumericNames = new HashMap<>();
        Map<RScoredSortedSet<Object>, Number> geNumericNames = new HashMap<>();
        Map<RScoredSortedSet<Object>, Number> ltNumericNames = new HashMap<>();
        Map<RScoredSortedSet<Object>, Number> leNumericNames = new HashMap<>();
        Map<RScoredSortedSet<Object>, Number> eqNumericNames = new HashMap<>();
        
        for (Condition cond : condition.getConditions()) {
            if (cond instanceof EQCondition) {
                EQCondition eqc = (EQCondition) cond;
                
                String indexName = namingScheme.getIndexName(entityClass, eqc.getName());
                if (eqc.getValue() instanceof Number && !isCollectionField(entityClass, eqc.getName())) {
                    RScoredSortedSet<Object> values = new RedissonScoredSortedSet<>(namingScheme.getCodec(), commandExecutor, indexName, null);
                    eqNumericNames.put(values, (Number) eqc.getValue());
                } else {
                    RSetMultimap<Object, Object> map = new RedissonSetMultimap<>(namingScheme.getCodec(), commandExecutor, indexName);
                    RSet<Object> values = map.get(eqc.getValue());
                    eqNames.add(((RedissonObject) values).getRawName());
                }
            }
            if (cond instanceof LTCondition) {
                LTCondition ltc = (LTCondition) cond;
                if (!handleRangeAnd(allIds, entityClass, ltc.getName(), ltc.getValue(),
                        "lt", ltNumericNames, namingScheme)) {
                    return Collections.emptySet();
                }
            }
            if (cond instanceof LECondition) {
                LECondition lec = (LECondition) cond;
                if (!handleRangeAnd(allIds, entityClass, lec.getName(), lec.getValue(),
                        "le", leNumericNames, namingScheme)) {
                    return Collections.emptySet();
                }
            }
            if (cond instanceof GECondition) {
                GECondition gec = (GECondition) cond;
                if (!handleRangeAnd(allIds, entityClass, gec.getName(), gec.getValue(),
                        "ge", geNumericNames, namingScheme)) {
                    return Collections.emptySet();
                }
            }
            if (cond instanceof GTCondition) {
                GTCondition gtc = (GTCondition) cond;
                if (!handleRangeAnd(allIds, entityClass, gtc.getName(), gtc.getValue(),
                        "gt", gtNumericNames, namingScheme)) {
                    return Collections.emptySet();
                }
            }
            
            if (cond instanceof ORCondition) {
                Collection<Object> ids = traverseOr((ORCondition) cond, namingScheme, entityClass);
                if (ids.isEmpty()) {
                    return Collections.emptySet();
                }
                if (!allIds.isEmpty()) {
                    allIds.retainAll(ids);
                } else {
                    allIds.addAll(ids);
                }
                if (allIds.isEmpty()) {
                    return Collections.emptySet();
                }
            }
        }
        
        if (!eqNames.isEmpty()) {
            RSet<Object> set = new RedissonSet<>(commandExecutor, eqNames.get(0), null);
            Set<Object> intersect = set.readIntersection(eqNames.toArray(new String[eqNames.size()]));
            if (!allIds.isEmpty()) {
                allIds.retainAll(intersect);
                if (allIds.isEmpty()) {
                    return Collections.emptySet();
                }
            } else {
                allIds.addAll(intersect);
            }
            if (allIds.isEmpty()) {
                return allIds;
            }
        }

        if (!checkValueRange(allIds, eqNumericNames, (r, v) -> {
            return r.valueRange(v.doubleValue(), true, v.doubleValue(), true);
        })) {
            return Collections.emptySet();
        }

        if (!checkValueRange(allIds, gtNumericNames, (r, v) -> {
            return r.valueRange(v.doubleValue(), false, Double.POSITIVE_INFINITY, false);
        })) {
            return Collections.emptySet();
        }

        if (!checkValueRange(allIds, geNumericNames, (r, v) -> {
            return r.valueRange(v.doubleValue(), true, Double.POSITIVE_INFINITY, false);
        })) {
            return Collections.emptySet();
        }
        
        if (!checkValueRange(allIds, ltNumericNames, (r, v) -> {
            return r.valueRange(Double.NEGATIVE_INFINITY, false, v.doubleValue(), false);
        })) {
            return Collections.emptySet();
        }

        if (!checkValueRange(allIds, leNumericNames, (r, v) -> {
            return r.valueRange(Double.NEGATIVE_INFINITY, false, v.doubleValue(), true);
        })) {
            return Collections.emptySet();
        }
        
        return allIds;
    }

    private boolean checkValueRange(Set<Object> allIds, Map<RScoredSortedSet<Object>, Number> numericNames, 
                        BiFunction<RScoredSortedSet<Object>, Number, Collection<Object>> func) {
        if (numericNames.isEmpty()) {
            return true;
        }

        for (Entry<RScoredSortedSet<Object>, Number> e : numericNames.entrySet()) {
            Collection<Object> gtIds = func.apply(e.getKey(), e.getValue());
            if (gtIds.isEmpty()) {
                return false;
            }
            if (!allIds.isEmpty()) {
                allIds.retainAll(gtIds);
                if (allIds.isEmpty()) {
                    return false;
                }
            } else {
                allIds.addAll(gtIds);
            }
        }
        return true;
    }

    private Set<Object> traverseOr(ORCondition condition, NamingScheme namingScheme, Class<?> entityClass) {
        Set<Object> allIds = new HashSet<Object>();
        
        List<String> eqNames = new ArrayList<String>();

        Map<RScoredSortedSet<Object>, Number> ltNumericNames = new HashMap<>();
        Map<RScoredSortedSet<Object>, Number> leNumericNames = new HashMap<>();
        Map<RScoredSortedSet<Object>, Number> gtNumericNames = new HashMap<>();
        Map<RScoredSortedSet<Object>, Number> geNumericNames = new HashMap<>();
        Map<RScoredSortedSet<Object>, Number> eqNumericNames = new HashMap<>();
        
        for (Condition cond : condition.getConditions()) {
            if (cond instanceof EQCondition) {
                EQCondition eqc = (EQCondition) cond;
                
                String indexName = namingScheme.getIndexName(entityClass, eqc.getName());
                if (eqc.getValue() instanceof Number && !isCollectionField(entityClass, eqc.getName())) {
                    RScoredSortedSet<Object> values = new RedissonScoredSortedSet<>(namingScheme.getCodec(), commandExecutor, indexName, null);
                    eqNumericNames.put(values, (Number) eqc.getValue());
                } else {
                    RSetMultimap<Object, Object> map = new RedissonSetMultimap<>(namingScheme.getCodec(), commandExecutor, indexName);
                    RSet<Object> values = map.get(eqc.getValue());
                    eqNames.add(((RedissonObject) values).getRawName());
                }
            }
            if (cond instanceof GTCondition) {
                GTCondition gtc = (GTCondition) cond;
                handleRangeOr(allIds, entityClass, gtc.getName(), gtc.getValue(),
                        "gt", gtNumericNames, namingScheme);
            }
            if (cond instanceof GECondition) {
                GECondition gec = (GECondition) cond;
                handleRangeOr(allIds, entityClass, gec.getName(), gec.getValue(),
                        "ge", geNumericNames, namingScheme);
            }
            if (cond instanceof LTCondition) {
                LTCondition ltc = (LTCondition) cond;
                handleRangeOr(allIds, entityClass, ltc.getName(), ltc.getValue(),
                        "lt", ltNumericNames, namingScheme);
            }
            if (cond instanceof LECondition) {
                LECondition lec = (LECondition) cond;
                handleRangeOr(allIds, entityClass, lec.getName(), lec.getValue(),
                        "le", leNumericNames, namingScheme);
            }
            if (cond instanceof ANDCondition) {
                Collection<Object> ids = traverseAnd((ANDCondition) cond, namingScheme, entityClass);
                allIds.addAll(ids);
            }
        }

        if (!eqNames.isEmpty()) {
            RSet<Object> set = new RedissonSet<>(commandExecutor, eqNames.get(0), null);
            allIds.addAll(set.readUnion(eqNames.toArray(new String[eqNames.size()])));
        }
        
        for (Entry<RScoredSortedSet<Object>, Number> e : eqNumericNames.entrySet()) {
            Collection<Object> ids = e.getKey().valueRange(e.getValue().doubleValue(), true, e.getValue().doubleValue(), true);
            allIds.addAll(ids);
        }
        
        for (Entry<RScoredSortedSet<Object>, Number> e : gtNumericNames.entrySet()) {
            Collection<Object> ids = e.getKey().valueRange(e.getValue().doubleValue(), false, Double.POSITIVE_INFINITY, false);
            allIds.addAll(ids);
        }

        for (Entry<RScoredSortedSet<Object>, Number> e : geNumericNames.entrySet()) {
            Collection<Object> ids = e.getKey().valueRange(e.getValue().doubleValue(), true, Double.POSITIVE_INFINITY, false);
            allIds.addAll(ids);
        }
        
        for (Entry<RScoredSortedSet<Object>, Number> e : ltNumericNames.entrySet()) {
            Collection<Object> ids = e.getKey().valueRange(Double.NEGATIVE_INFINITY, false, e.getValue().doubleValue(), false);
            allIds.addAll(ids);
        }

        for (Entry<RScoredSortedSet<Object>, Number> e : leNumericNames.entrySet()) {
            Collection<Object> ids = e.getKey().valueRange(Double.NEGATIVE_INFINITY, false, e.getValue().doubleValue(), true);
            allIds.addAll(ids);
        }

        return allIds;
    }
    
    public Set<Object> find(Class<?> entityClass, Condition condition) {
        NamingScheme namingScheme = commandExecutor.getObjectBuilder().getNamingScheme(entityClass);

        if (condition instanceof EQCondition) {
            EQCondition c = (EQCondition) condition;
            String indexName = namingScheme.getIndexName(entityClass, c.getName());
            
            if (c.getValue() instanceof Number && !isCollectionField(entityClass, c.getName())) {
                RScoredSortedSet<Object> set = new RedissonScoredSortedSet<>(namingScheme.getCodec(), commandExecutor, indexName, null);
                double v = ((Number) c.getValue()).doubleValue();
                Collection<Object> gtIds = set.valueRange(v, true, v, true);
                return new HashSet<>(gtIds);
            } else {
                RSetMultimap<Object, Object> map = new RedissonSetMultimap<>(namingScheme.getCodec(), commandExecutor, indexName);
                return map.getAll(c.getValue());
            }
        } else if (condition instanceof GTCondition) {
            GTCondition c = (GTCondition) condition;
            if (isCollectionField(entityClass, c.getName())) {
                return collectionRangeFind(entityClass, c.getName(), c.getValue(), "gt");
            }
            String indexName = namingScheme.getIndexName(entityClass, c.getName());
            RScoredSortedSet<Object> set = new RedissonScoredSortedSet<>(namingScheme.getCodec(), commandExecutor, indexName, null);
            Collection<Object> gtIds = set.valueRange(c.getValue().doubleValue(), false, Double.POSITIVE_INFINITY, false);
            return new HashSet<>(gtIds);
        } else if (condition instanceof GECondition) {
            GECondition c = (GECondition) condition;
            if (isCollectionField(entityClass, c.getName())) {
                return collectionRangeFind(entityClass, c.getName(), c.getValue(), "ge");
            }
            String indexName = namingScheme.getIndexName(entityClass, c.getName());
            RScoredSortedSet<Object> set = new RedissonScoredSortedSet<>(namingScheme.getCodec(), commandExecutor, indexName, null);
            Collection<Object> gtIds = set.valueRange(c.getValue().doubleValue(), true, Double.POSITIVE_INFINITY, false);
            return new HashSet<>(gtIds);
        } else if (condition instanceof LTCondition) {
            LTCondition c = (LTCondition) condition;
            if (isCollectionField(entityClass, c.getName())) {
                return collectionRangeFind(entityClass, c.getName(), c.getValue(), "lt");
            }
            String indexName = namingScheme.getIndexName(entityClass, c.getName());
            RScoredSortedSet<Object> set = new RedissonScoredSortedSet<>(namingScheme.getCodec(), commandExecutor, indexName, null);
            Collection<Object> gtIds = set.valueRange(Double.NEGATIVE_INFINITY, false, c.getValue().doubleValue(), false);
            return new HashSet<>(gtIds);
        } else if (condition instanceof LECondition) {
            LECondition c = (LECondition) condition;
            if (isCollectionField(entityClass, c.getName())) {
                return collectionRangeFind(entityClass, c.getName(), c.getValue(), "le");
            }
            String indexName = namingScheme.getIndexName(entityClass, c.getName());
            RScoredSortedSet<Object> set = new RedissonScoredSortedSet<>(namingScheme.getCodec(), commandExecutor, indexName, null);
            Collection<Object> gtIds = set.valueRange(Double.NEGATIVE_INFINITY, false, c.getValue().doubleValue(), true);
            return new HashSet<>(gtIds);
        } else if (condition instanceof ORCondition) {
            return traverseOr((ORCondition) condition, namingScheme, entityClass);
        } else if (condition instanceof ANDCondition) {
            return traverseAnd((ANDCondition) condition, namingScheme, entityClass);
        }

        throw new IllegalArgumentException();
    }

    
}

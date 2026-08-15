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
package org.redisson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.*;

import org.junit.jupiter.api.Test;
import org.redisson.api.RLiveObjectService;
import org.redisson.api.annotation.REntity;
import org.redisson.api.annotation.RId;
import org.redisson.api.annotation.RIndex;
import org.redisson.api.condition.Conditions;

public class CollectionSyncProxyTest extends RedisDockerTest {

    @REntity
    public static class TaggedItem {

        @RId
        private String id;
        @RIndex
        private List<String> tags;
        @RIndex
        private List<Integer> values;

        public TaggedItem() {
        }

        public TaggedItem(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }

        public List<Integer> getValues() {
            return values;
        }

        public void setValues(List<Integer> values) {
            this.values = values;
        }
    }

    @Test
    public void testAddAndRemove() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setTags(new ArrayList<>(Arrays.asList("a", "b")));
        item = s.persist(item);

        item.getTags().add("c");
        item.getTags().remove("a");

        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "a"))).isEmpty();
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "b"))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "c"))).hasSize(1);
    }

    @Test
    public void testClear() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setTags(new ArrayList<>(Arrays.asList("a", "b")));
        item = s.persist(item);

        item.getTags().clear();

        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "a"))).isEmpty();
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "b"))).isEmpty();
    }

    @Test
    public void testAddAll() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setTags(new ArrayList<>(Arrays.asList("a")));
        item = s.persist(item);

        item.getTags().addAll(Arrays.asList("b", "c"));

        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "a"))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "b"))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "c"))).hasSize(1);
    }

    @Test
    public void testRemoveAll() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setTags(new ArrayList<>(Arrays.asList("a", "b", "c")));
        item = s.persist(item);

        item.getTags().removeAll(Arrays.asList("a", "c"));

        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "a"))).isEmpty();
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "b"))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "c"))).isEmpty();
    }

    @Test
    public void testSetByIndex() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setValues(new ArrayList<>(Arrays.asList(10, 20)));
        item = s.persist(item);

        item.getValues().set(0, 99);

        Collection<TaggedItem> result = s.find(TaggedItem.class, Conditions.eq("values", 10));
        assertThat(result).isEmpty();

        result = s.find(TaggedItem.class, Conditions.eq("values", 99));
        assertThat(result).hasSize(1);
        assertThat(result.iterator().next().getId()).isEqualTo("1");
    }

    @Test
    public void testAddByIndex() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setValues(new ArrayList<>(Arrays.asList(10, 20)));
        item = s.persist(item);

        item.getValues().add(0, 99);

        Collection<TaggedItem> result = s.find(TaggedItem.class, Conditions.eq("values", 99));
        assertThat(result).hasSize(1);
        assertThat(result.iterator().next().getId()).isEqualTo("1");
    }

    @Test
    public void testAddAllByIndex() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setValues(new ArrayList<>(Arrays.asList(10, 20)));
        item = s.persist(item);

        item.getValues().addAll(0, Arrays.asList(99, 88));

        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 99))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 88))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 10))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 20))).hasSize(1);
    }

    @Test
    public void testIteratorRemove() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setTags(new ArrayList<>(Arrays.asList("a", "b", "c")));
        item = s.persist(item);

        Iterator<String> it = item.getTags().iterator();
        while (it.hasNext()) {
            String tag = it.next();
            if ("b".equals(tag)) {
                it.remove();
            }
        }

        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "a"))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "b"))).isEmpty();
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "c"))).hasSize(1);
    }

    @Test
    public void testListIteratorSet() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setValues(new ArrayList<>(Arrays.asList(10, 20)));
        item = s.persist(item);

        ListIterator<Integer> it = item.getValues().listIterator();
        while (it.hasNext()) {
            Integer val = it.next();
            if (val == 10) {
                it.set(99);
            }
        }

        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 10))).isEmpty();
        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 99))).hasSize(1);
    }

    @Test
    public void testListIteratorConsecutiveSet() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setValues(new ArrayList<>(Arrays.asList(10)));
        item = s.persist(item);

        ListIterator<Integer> it = item.getValues().listIterator();
        it.next();
        it.set(99);
        it.set(100);

        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 10))).isEmpty();
        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 99))).isEmpty();
        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 100))).hasSize(1);
    }

    @Test
    public void testListIteratorPreviousSet() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setValues(new ArrayList<>(Arrays.asList(10, 20, 30)));
        item = s.persist(item);

        ListIterator<Integer> it = item.getValues().listIterator();
        it.next(); it.next(); it.next(); // forward to end
        assertThat(it.previous()).isEqualTo(30);
        assertThat(it.previous()).isEqualTo(20);
        assertThat(it.previous()).isEqualTo(10);
        assertThat(it.hasPrevious()).isFalse();
    }

    @Test
    public void testListIteratorAdd() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setValues(new ArrayList<>(Arrays.asList(10, 20)));
        item = s.persist(item);

        ListIterator<Integer> it = item.getValues().listIterator();
        while (it.hasNext()) {
            it.next();
        }
        it.add(99);

        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 99))).hasSize(1);
    }

    @Test
    public void testReadOnlyMethodsDoNotAffectIndex() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setTags(new ArrayList<>(Arrays.asList("a", "b")));
        item = s.persist(item);

        item.getTags().contains("a");
        item.getTags().size();
        item.getTags().isEmpty();
        item.getTags().get(0);

        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "a"))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "b"))).hasSize(1);
    }

    @Test
    public void testRemoveAllWithNonExistentElements() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setTags(new ArrayList<>(Arrays.asList("a", "b")));
        item = s.persist(item);

        item.getTags().removeAll(Arrays.asList("a", "z"));

        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "a"))).isEmpty();
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "b"))).hasSize(1);
    }

    @Test
    public void testRemoveByIndex() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setValues(new ArrayList<>(Arrays.asList(10, 20, 30)));
        item = s.persist(item);

        item.getValues().remove(0);

        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 10))).isEmpty();
        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 20))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 30))).hasSize(1);
    }

    @Test
    public void testListIteratorFromIndex() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setValues(new ArrayList<>(Arrays.asList(10, 20, 30)));
        item = s.persist(item);

        ListIterator<Integer> it = item.getValues().listIterator(1);
        while (it.hasNext()) {
            Integer val = it.next();
            if (val == 20) {
                it.set(99);
            }
        }

        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 10))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 20))).isEmpty();
        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 99))).hasSize(1);
    }

    @Test
    public void testRemoveReturnsFalse() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setTags(new ArrayList<>(Arrays.asList("a", "b")));
        item = s.persist(item);

        item.getTags().remove("z");

        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "a"))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "b"))).hasSize(1);
    }

    @Test
    public void testRemoveAllReturnsFalse() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setTags(new ArrayList<>(Arrays.asList("a", "b")));
        item = s.persist(item);

        item.getTags().removeAll(Arrays.asList("x", "y"));

        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "a"))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "b"))).hasSize(1);
    }

    @Test
    public void testRejectUnsupportedMutatingMethods() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem entity = s.persist(new TaggedItem("1"));
        entity.setTags(new ArrayList<>(Arrays.asList("a", "b")));

        assertThatThrownBy(() ->
                entity.getTags().removeIf(tag -> "a".equals(tag)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("removeIf");

        assertThatThrownBy(() ->
                entity.getTags().retainAll(Arrays.asList("a")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("retainAll");

        assertThatThrownBy(() ->
                entity.getTags().replaceAll(tag -> "z"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("replaceAll");

        assertThatThrownBy(() ->
                entity.getTags().sort(String::compareTo))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("sort");

        // Index must remain unchanged after all rejections
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "a"))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("tags", "b"))).hasSize(1);
    }

    @Test
    public void testIndexIsolationBetweenEntities() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item1 = s.persist(new TaggedItem("1"));
        TaggedItem item2 = s.persist(new TaggedItem("2"));

        item1.setTags(new ArrayList<>(Arrays.asList("java")));
        item2.setTags(new ArrayList<>(Arrays.asList("java", "redis")));

        item1.getTags().remove("java");

        Collection<TaggedItem> result = s.find(TaggedItem.class, Conditions.eq("tags", "java"));
        assertThat(result).hasSize(1);
        assertThat(result.iterator().next().getId()).isEqualTo("2");
    }

    @Test
    public void testRemoveByObjectIntegerDisambiguation() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setValues(new ArrayList<>(Arrays.asList(10, 20, 30)));
        item = s.persist(item);

        item.getValues().remove(Integer.valueOf(20));

        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 10))).hasSize(1);
        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 20))).isEmpty();
        assertThat(s.find(TaggedItem.class, Conditions.eq("values", 30))).hasSize(1);
    }

    @Test
    public void testSubListReadOnly() {
        RLiveObjectService s = redisson.getLiveObjectService();

        TaggedItem item = new TaggedItem("1");
        item.setValues(new ArrayList<>(Arrays.asList(10, 20, 30)));
        item = s.persist(item);

        List<Integer> sub = item.getValues().subList(0, 2);

        assertThat(sub.get(0)).isEqualTo(10);
        assertThat(sub.get(1)).isEqualTo(20);
        assertThat(sub.size()).isEqualTo(2);
        assertThat(sub.contains(10)).isTrue();
        assertThat(sub.contains(30)).isFalse();

        assertThatThrownBy(() -> sub.add(99))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> sub.remove(Integer.valueOf(10)))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> sub.clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}

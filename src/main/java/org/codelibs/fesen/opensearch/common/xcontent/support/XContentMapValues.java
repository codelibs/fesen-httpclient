/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.codelibs.fesen.opensearch.common.xcontent.support;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.Booleans;
import org.codelibs.fesen.opensearch.common.Numbers;
import org.codelibs.fesen.opensearch.common.regex.Regex;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.Strings;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Map values for xcontent parsing.
 *
 * @opensearch.internal
 */
public class XContentMapValues {
    /**
     * Creates a new XContentMapValues.
     */
    public XContentMapValues() {
    }

    private static final String TRANSFORMER_TRIE_LEAF_KEY = "$transformer";

    /**
     * Extracts raw values (string, int, and so on) based on the path provided returning all of them
     * as a single list.
     *
     * @param path the path
     * @param map the map
     * @return the extract raw values
     */
    public static List<Object> extractRawValues(String path, Map<String, Object> map) {
        List<Object> values = new ArrayList<>();
        String[] pathElements = path.split("\\.");
        if (pathElements.length == 0) {
            return values;
        }
        extractRawValues(values, map, pathElements, 0);
        return values;
    }

    @SuppressWarnings({ "unchecked" })
    private static void extractRawValues(List values, Map<String, Object> part, String[] pathElements, int index) {
        if (index == pathElements.length) {
            return;
        }

        String key = pathElements[index];
        Object currentValue = part.get(key);
        int nextIndex = index + 1;
        while (currentValue == null && nextIndex != pathElements.length) {
            key += "." + pathElements[nextIndex];
            currentValue = part.get(key);
            nextIndex++;
        }

        if (currentValue == null) {
            return;
        }

        if (currentValue instanceof Map) {
            extractRawValues(values, (Map<String, Object>) currentValue, pathElements, nextIndex);
        } else if (currentValue instanceof List) {
            extractRawValues(values, (List) currentValue, pathElements, nextIndex);
        } else {
            values.add(currentValue);
        }
    }

    @SuppressWarnings({ "unchecked" })
    private static void extractRawValues(List values, List<Object> part, String[] pathElements, int index) {
        for (Object value : part) {
            if (value == null) {
                continue;
            }
            if (value instanceof Map) {
                extractRawValues(values, (Map<String, Object>) value, pathElements, index);
            } else if (value instanceof List) {
                extractRawValues(values, (List) value, pathElements, index);
            } else {
                values.add(value);
            }
        }
    }

    private static int step(CharacterRunAutomaton automaton, String key, int state) {
        for (int i = 0; state != -1 && i < key.length(); ++i) {
            state = automaton.step(state, key.charAt(i));
        }
        return state;
    }

    private static Map<String, Object> filter(
        Map<String, ?> map,
        CharacterRunAutomaton includeAutomaton,
        int initialIncludeState,
        CharacterRunAutomaton excludeAutomaton,
        int initialExcludeState,
        CharacterRunAutomaton matchAllAutomaton,
        boolean caseSensitive
    ) {
        Map<String, Object> filtered = new HashMap<>();
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            String key = entry.getKey();
            String k = caseSensitive ? key : key.toLowerCase(Locale.ROOT);

            int includeState = step(includeAutomaton, k, initialIncludeState);
            if (includeState == -1) {
                continue;
            }

            int excludeState = step(excludeAutomaton, k, initialExcludeState);
            if (excludeState != -1 && excludeAutomaton.isAccept(excludeState)) {
                continue;
            }

            Object value = entry.getValue();

            CharacterRunAutomaton subIncludeAutomaton = includeAutomaton;
            int subIncludeState = includeState;
            if (includeAutomaton.isAccept(includeState)) {
                if (excludeState == -1 || excludeAutomaton.step(excludeState, '.') == -1) {
                    // the exclude has no chances to match inner properties
                    filtered.put(key, value);
                    continue;
                } else {
                    // the object matched, so consider that the include matches every inner property
                    // we only care about excludes now
                    subIncludeAutomaton = matchAllAutomaton;
                    subIncludeState = 0;
                }
            }

            if (value instanceof Map) {

                subIncludeState = subIncludeAutomaton.step(subIncludeState, '.');
                if (subIncludeState == -1) {
                    continue;
                }
                if (excludeState != -1) {
                    excludeState = excludeAutomaton.step(excludeState, '.');
                }

                Map<String, Object> valueAsMap = (Map<String, Object>) value;
                Map<String, Object> filteredValue = filter(
                    valueAsMap,
                    subIncludeAutomaton,
                    subIncludeState,
                    excludeAutomaton,
                    excludeState,
                    matchAllAutomaton,
                    caseSensitive
                );
                if (includeAutomaton.isAccept(includeState) || filteredValue.isEmpty() == false) {
                    filtered.put(key, filteredValue);
                }

            } else if (value instanceof Iterable) {

                List<Object> filteredValue = filter(
                    (Iterable<?>) value,
                    subIncludeAutomaton,
                    subIncludeState,
                    excludeAutomaton,
                    excludeState,
                    matchAllAutomaton,
                    caseSensitive
                );
                if (includeAutomaton.isAccept(includeState) || filteredValue.isEmpty() == false) {
                    filtered.put(key, filteredValue);
                }

            } else {

                // leaf property
                if (includeAutomaton.isAccept(includeState) && (excludeState == -1 || excludeAutomaton.isAccept(excludeState) == false)) {
                    filtered.put(key, value);
                }

            }

        }
        return filtered;
    }

    private static List<Object> filter(
        Iterable<?> iterable,
        CharacterRunAutomaton includeAutomaton,
        int initialIncludeState,
        CharacterRunAutomaton excludeAutomaton,
        int initialExcludeState,
        CharacterRunAutomaton matchAllAutomaton,
        boolean caseSensitive
    ) {
        List<Object> filtered = new ArrayList<>();
        boolean isInclude = includeAutomaton.isAccept(initialIncludeState);
        for (Object value : iterable) {
            if (value instanceof Map) {
                int includeState = includeAutomaton.step(initialIncludeState, '.');
                int excludeState = initialExcludeState;
                if (excludeState != -1) {
                    excludeState = excludeAutomaton.step(excludeState, '.');
                }
                Map<String, Object> filteredValue = filter(
                    (Map<String, ?>) value,
                    includeAutomaton,
                    includeState,
                    excludeAutomaton,
                    excludeState,
                    matchAllAutomaton,
                    caseSensitive
                );
                if (filteredValue.isEmpty() == false) {
                    filtered.add(filteredValue);
                }
            } else if (value instanceof Iterable) {
                List<Object> filteredValue = filter(
                    (Iterable<?>) value,
                    includeAutomaton,
                    initialIncludeState,
                    excludeAutomaton,
                    initialExcludeState,
                    matchAllAutomaton,
                    caseSensitive
                );
                if (filteredValue.isEmpty() == false) {
                    filtered.add(filteredValue);
                }
            } else if (isInclude) {
                // #22557: only accept this array value if the key we are on is accepted:
                filtered.add(value);
            }
        }
        return filtered;
    }

    /**
     * Returns the node boolean value.
     *
     * @param node the node
     * @return the node boolean value
     */
    public static boolean nodeBooleanValue(Object node) {
        return Booleans.parseBoolean(node.toString());
    }

    /**
     * Returns the node map value.
     *
     * @param node the node
     * @param desc the desc
     * @return the node map value
     */
    public static Map<String, Object> nodeMapValue(Object node, String desc) {
        if (node instanceof Map) {
            return (Map<String, Object>) node;
        } else {
            throw new OpenSearchParseException(desc + " should be a hash but was of type: " + node.getClass());
        }
    }

    private static void processStack(Deque<TransformContext> stack, boolean inPlace) {
        while (!stack.isEmpty()) {
            TransformContext ctx = stack.pop();
            processMap(ctx.map, ctx.trie, stack, inPlace);
        }
    }

    private static void processMap(
        Map<String, Object> currentMap,
        Map<String, Object> currentTrie,
        Deque<TransformContext> stack,
        boolean inPlace
    ) {
        for (Map.Entry<String, Object> entry : currentMap.entrySet()) {
            processEntry(entry, currentTrie, stack, inPlace);
        }
    }

    private static void processEntry(
        Map.Entry<String, Object> entry,
        Map<String, Object> currentTrie,
        Deque<TransformContext> stack,
        boolean inPlace
    ) {
        String key = entry.getKey();
        Object value = entry.getValue();

        Object subTrieObj = currentTrie.get(key);
        if (subTrieObj instanceof Map == false) {
            return;
        }
        Map<String, Object> subTrie = nodeMapValue(subTrieObj, "transform");

        // Apply transformation if available
        Function<Object, Object> transformer = (Function<Object, Object>) subTrie.get(TRANSFORMER_TRIE_LEAF_KEY);
        if (transformer != null) {
            entry.setValue(transformer.apply(value));
            return;
        }

        // Process nested structures
        if (value instanceof Map) {
            Map<String, Object> subMap = nodeMapValue(value, "transform");
            if (inPlace == false) {
                subMap = new HashMap<>(subMap);
                entry.setValue(subMap);
            }
            stack.push(new TransformContext(subMap, subTrie));
        } else if (value instanceof List<?> list) {
            List<Object> subList = (List<Object>) list;
            if (inPlace == false) {
                subList = new ArrayList<>(list);
                entry.setValue(subList);
            }
            processList(subList, subTrie, stack, inPlace);
        }
    }

    private static void processList(
        List<Object> list,
        Map<String, Object> transformerTrie,
        Deque<TransformContext> stack,
        boolean inPlace
    ) {
        for (int i = list.size() - 1; i >= 0; i--) {
            Object value = list.get(i);
            if (value instanceof Map) {
                Map<String, Object> subMap = nodeMapValue(value, "transform");
                if (inPlace == false) {
                    subMap = new HashMap<>(subMap);
                    list.set(i, subMap);
                }
                stack.push(new TransformContext(subMap, transformerTrie));
            }
        }
    }

    private static class TransformContext {
        Map<String, Object> map;
        Map<String, Object> trie;

        TransformContext(Map<String, Object> map, Map<String, Object> trie) {
            this.map = map;
            this.trie = trie;
        }
    }
}

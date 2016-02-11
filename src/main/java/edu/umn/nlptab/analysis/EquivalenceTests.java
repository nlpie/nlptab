/*
 * Copyright (c) 2015 Regents of the University of Minnesota.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umn.nlptab.analysis;


import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import static edu.umn.nlptab.analysis.EquivalenceTest.*;

/**
 */
final class EquivalenceTests {
    private EquivalenceTests() {
        throw new UnsupportedOperationException();
    }

    private static final Map<String, EquivalenceTest> EQUIVALENCE_TESTS_MAP = ImmutableMap
            .<String, EquivalenceTest>builder()
            .put("any are in", ANY_ARE_IN)
            .put("none are in", NONE_ARE_IN)
            .put("all are in", ALL_ARE_IN)
            .put("covers all", COVERS_ALL)
            .put("equals", EQUALS)
            .put("any are equal to", ANY_ARE_EQUAL_TO)
            .put("none are equal to", NONE_ARE_EQUAL_TO)
            .put("is in", IS_IN)
            .put("is not in", IS_NOT_IN)
            .put("does not equal", DOES_NOT_EQUAL)
            .build();

    public static EquivalenceTest getEquivalenceTest(String equivalenceTestString) {
        EquivalenceTest equivalenceTest = EQUIVALENCE_TESTS_MAP.get(equivalenceTestString);
        if (equivalenceTest == null) {
            throw new IllegalArgumentException("Invalid equivalence test string: " + equivalenceTestString);
        }
        return equivalenceTest;
    }

    private static final Map<EquivalenceTest, EquivalenceTest> CONVERSES = buildConverses();

    private static Map<EquivalenceTest, EquivalenceTest> buildConverses() {
        Map<EquivalenceTest, EquivalenceTest> converses = new EnumMap<>(EquivalenceTest.class);
        converses.put(ANY_ARE_IN, ANY_ARE_IN);
        converses.put(NONE_ARE_IN, NONE_ARE_IN);
        converses.put(ALL_ARE_IN, COVERS_ALL);
        converses.put(COVERS_ALL, ALL_ARE_IN);
        converses.put(EQUALS, EQUALS);
        converses.put(ANY_ARE_EQUAL_TO, IS_IN);
        converses.put(NONE_ARE_EQUAL_TO, IS_NOT_IN);
        converses.put(IS_IN, ANY_ARE_EQUAL_TO);
        converses.put(IS_NOT_IN, NONE_ARE_EQUAL_TO);
        converses.put(DOES_NOT_EQUAL, DOES_NOT_EQUAL);
        return Collections.unmodifiableMap(converses);
    }

    public static EquivalenceTest getConverse(EquivalenceTest equivalenceTest) {
        return CONVERSES.get(equivalenceTest);
    }
}

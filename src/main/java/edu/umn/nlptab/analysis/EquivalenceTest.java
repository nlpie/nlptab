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

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;

/**
 *
 */
public enum EquivalenceTest {
    ANY_ARE_IN {
        @Override
        boolean test(@Nullable Object hypothesisValue, @Nullable Object referenceValue) {
            if (hypothesisValue instanceof Collection) {
                Collection<?> hypothesisCollection = (Collection<?>) hypothesisValue;
                if (referenceValue instanceof Collection) {
                    Collection<?> referenceCollection = (Collection<?>) referenceValue;
                    return hypothesisCollection.stream().anyMatch(referenceCollection::contains);
                } else {
                    return EQUALS.test(hypothesisValue, referenceValue);
                }
            } else {
                return IS_IN.test(hypothesisValue, referenceValue);
            }
        }
    },
    NONE_ARE_IN {
        @Override
        boolean test(@Nullable Object hypothesisValue, @Nullable Object referenceValue) {
            if (hypothesisValue instanceof Collection) {
                Collection<?> hypothesisCollection = (Collection<?>) hypothesisValue;
                if (referenceValue instanceof Collection) {
                    Collection<?> referenceCollection = (Collection<?>) referenceValue;
                    return hypothesisCollection.stream().noneMatch(referenceCollection::contains);
                } else {
                    return DOES_NOT_EQUAL.test(hypothesisValue, referenceValue);
                }
            } else {
                return IS_NOT_IN.test(hypothesisValue, referenceValue);
            }
        }
    },
    ALL_ARE_IN {
        @Override
        boolean test(@Nullable Object hypothesisValue, @Nullable Object referenceValue) {
            if (hypothesisValue instanceof Collection) {
                Collection<?> hypothesisCollection = (Collection<?>) hypothesisValue;
                if (referenceValue instanceof Collection) {
                    Collection<?> referenceCollection = (Collection<?>) referenceValue;
                    return hypothesisCollection.stream().allMatch(referenceCollection::contains);
                } else {
                    return EQUALS.test(hypothesisValue, referenceValue);
                }
            } else {
                return IS_IN.test(hypothesisValue, referenceValue);
            }
        }
    },
    COVERS_ALL {
        @Override
        boolean test(@Nullable Object hypothesisValue, @Nullable Object referenceValue) {
            if (hypothesisValue instanceof Collection) {
                Collection<?> hypothesisCollection = (Collection<?>) hypothesisValue;
                if (referenceValue instanceof Collection) {
                    Collection<?> referenceCollection = (Collection<?>) referenceValue;
                    return referenceCollection.stream().allMatch(hypothesisCollection::contains);
                } else {
                    return EQUALS.test(hypothesisValue, referenceValue);
                }
            } else {
                return IS_EQUAL_TO_ALL.test(hypothesisValue, referenceValue);
            }
        }
    },
    EQUALS {
        @Override
        boolean test(@Nullable Object hypothesisValue, @Nullable Object referenceValue) {
            return Objects.equals(hypothesisValue, referenceValue);
        }
    },
    IS_EQUAL_TO_ALL {
        @Override
        boolean test(@Nullable Object hypothesisValue, @Nullable Object referenceValue) {
            if (referenceValue instanceof Collection) {
                Collection<?> referenceCollection = (Collection<?>) referenceValue;
                return referenceCollection.stream().allMatch(r -> Objects.equals(hypothesisValue, r));
            } else {
                return EQUALS.test(hypothesisValue, referenceValue);
            }
        }
    },
    ANY_ARE_EQUAL_TO {
        @Override
        boolean test(@Nullable Object hypothesisValue, @Nullable Object referenceValue) {
            if (hypothesisValue instanceof Collection) {
                Collection hypothesisCollection = (Collection) hypothesisValue;
                return hypothesisCollection.contains(referenceValue);
            } else {
                return EQUALS.test(hypothesisValue, referenceValue);
            }
        }
    },
    NONE_ARE_EQUAL_TO {
        @Override
        boolean test(@Nullable Object hypothesisValue, @Nullable Object referenceValue) {
            if (hypothesisValue instanceof Collection) {
                Collection hypothesisCollection = (Collection) hypothesisValue;
                return !hypothesisCollection.contains(referenceValue);
            } else {
                return DOES_NOT_EQUAL.test(hypothesisValue, referenceValue);
            }
        }
    },
    IS_IN {
        @Override
        boolean test(@Nullable Object hypothesisValue, @Nullable Object referenceValue) {
            if (referenceValue instanceof Collection) {
                Collection referenceCollection = (Collection) referenceValue;
                return referenceCollection.contains(hypothesisValue);
            } else {
                return EQUALS.test(hypothesisValue, referenceValue);
            }
        }
    },
    IS_NOT_IN {
        @Override
        boolean test(@Nullable Object hypothesisValue, @Nullable Object referenceValue) {
            if (referenceValue instanceof Collection) {
                Collection referenceCollection = (Collection) referenceValue;
                return !referenceCollection.contains(hypothesisValue);
            } else {
                return DOES_NOT_EQUAL.test(hypothesisValue, referenceValue);
            }
        }
    },
    DOES_NOT_EQUAL {
        @Override
        boolean test(@Nullable Object hypothesisValue, @Nullable Object referenceValue) {
            return !Objects.equals(hypothesisValue, referenceValue);
        }
    };

    abstract boolean test(@Nullable Object hypothesisValue, @Nullable Object referenceValue);
}
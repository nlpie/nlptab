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

package edu.umn.nlptab.uimatyping;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.TypeSystem;

import java.util.Iterator;

/**
 *
 */
public class TypeFilterBuilder {
    private TypeFilterLists typeFilterLists;
    private TypeSystem typeSystem;

    public static TypeFilterBuilder newBuilder() {
        return new TypeFilterBuilder();
    }

    public TypeFilterBuilder withTypeFilterLists(TypeFilterLists typeFilterLists) {
        this.typeFilterLists = typeFilterLists;
        return this;
    }

    public TypeFilterBuilder withTypeSystem(TypeSystem typeSystem) {
        this.typeSystem = typeSystem;
        return this;
    }

    public ImmutableSet<String> createTypeFilter() {
        ImmutableSortedSet.Builder<String> acceptedTypesBuilder = ImmutableSortedSet.naturalOrder();

        Iterator<Type> typeIterator = typeSystem.getTypeIterator();
        while (typeIterator.hasNext()) {
            Type next = typeIterator.next();
            if (typeFilterLists.shouldAcceptType(next, typeSystem)) {
                acceptedTypesBuilder.add(next.getName());
            }
        }
        return acceptedTypesBuilder.build();
    }
}

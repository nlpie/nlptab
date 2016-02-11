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
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.TypeSystem;

/**
 *
 */
public class TypeFilterLists {
    private final ImmutableSet<String> typeWhitelist;
    private final ImmutableSet<String> typeBlacklist;

    TypeFilterLists(ImmutableSet<String> typeWhitelist,
                           ImmutableSet<String> typeBlacklist) {
        this.typeWhitelist = typeWhitelist;
        this.typeBlacklist = typeBlacklist;
    }

    public static TypeFilterLists create(String[] typeWhitelist, String[] typeBlacklist) {
        ImmutableSet<String> whitelistSet;
        if (typeWhitelist.length == 0) {
            whitelistSet = ImmutableSortedSet.of(CAS.TYPE_NAME_TOP);
        } else {
            whitelistSet = ImmutableSortedSet.copyOf(typeWhitelist);
        }
        ImmutableSet<String> blacklistSet = ImmutableSortedSet.copyOf(typeBlacklist);
        return new TypeFilterLists(whitelistSet, blacklistSet);
    }

    public boolean shouldAcceptType(Type type, TypeSystem typeSystem) {
        Type parentType = type;
        int acceptedParentDistance = Integer.MAX_VALUE;
        int deniedParentDistance = Integer.MAX_VALUE;
        int parentDistance = 0;
        while (parentType != null) {
            String parentTypeName = parentType.getName();
            if (typeWhitelist.contains(parentTypeName)) {
                acceptedParentDistance = Math.min(acceptedParentDistance, parentDistance);
            }

            if (typeBlacklist.contains(parentTypeName)) {
                deniedParentDistance = Math.min(deniedParentDistance, parentDistance);
            }

            parentType = typeSystem.getParent(parentType);
            parentDistance++;
        }

        return acceptedParentDistance <= deniedParentDistance;
    }
}

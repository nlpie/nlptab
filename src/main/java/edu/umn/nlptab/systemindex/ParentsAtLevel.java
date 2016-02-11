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

package edu.umn.nlptab.systemindex;

import edu.umn.nlptab.casprocessing.FsDocumentLocation;

import java.util.*;

/**
 *
 */
class ParentsAtLevel {
    private final Set<FsDocumentLocation> locations;
    private final Collection<String> parents;
    private final Map<String, Collection<String>> childToParentMap;
    private final Map<String, FsDocumentLocation> documentLocationMap;
    private final Set<String> seenParents;

    ParentsAtLevel(Map<String, Collection<String>> childToParentMap, Map<String, FsDocumentLocation> documentLocationMap, Set<String> seenParents, Collection<String> nextParents) {
        locations = new HashSet<>();
        parents = nextParents;
        this.childToParentMap = childToParentMap;
        this.documentLocationMap = documentLocationMap;
        this.seenParents = seenParents;
    }

    Collection<String> invoke() {
        Collection<String> nextParents = new ArrayList<>();
        for (String parent : parents) {
            if (seenParents.contains(parent)) {
                continue;
            }
            seenParents.add(parent);
            FsDocumentLocation fsDocumentLocation = documentLocationMap.get(parent);
            if (fsDocumentLocation != null) {
                locations.add(fsDocumentLocation);
            }

            Collection<String> parentsParents = childToParentMap.get(parent);
            if (parentsParents != null) {
                nextParents.addAll(parentsParents);
            }
        }
        return nextParents;
    }

    Set<FsDocumentLocation> getLocations() {
        return locations;
    }
}

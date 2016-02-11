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


class DocumentLocationsForDocument {
    private final SortedMap<Integer, Set<FsDocumentLocation>> locationAtDistance;
    private final String key;
    private final Set<String> seenParents;
    private final Map<String, Collection<String>> childToParentMap;
    private final Map<String, FsDocumentLocation> documentLocationMap;

    DocumentLocationsForDocument(Map<String, Collection<String>> childToParentMap, Map<String, FsDocumentLocation> documentLocationMap, String key) {
        this.key = key;
        seenParents = new HashSet<>();
        locationAtDistance = new TreeMap<>();
        this.childToParentMap = childToParentMap;
        this.documentLocationMap = documentLocationMap;
    }

    void invoke() {
        FsDocumentLocation zeroLocation = documentLocationMap.get(key);
        if (zeroLocation != null) {
            locationAtDistance.put(0, Collections.singleton(zeroLocation));
        }

        int distance = 1;
        Collection<String> nextParents = childToParentMap.get(key);
        while (nextParents != null && !nextParents.isEmpty()) {
            ParentsAtLevel parentsAtLevel = new ParentsAtLevel(childToParentMap, documentLocationMap, seenParents, nextParents);
            nextParents = parentsAtLevel.invoke();
            Set<FsDocumentLocation> locations = parentsAtLevel.getLocations();
            if (locations.size() > 0) {
                locationAtDistance.put(distance, locations);
            }
            distance++;
        }
    }

    SortedMap<Integer, Set<FsDocumentLocation>> getLocationAtDistance() {
        return locationAtDistance;
    }
}

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

package edu.umn.nlptab.casprocessing;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.SofaFS;
import org.apache.uima.cas.Type;
import org.elasticsearch.common.Strings;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

/**
 *
 */
public class SofaData {
    /**
     * A seed for the murmur 128 hash function used to generated sofa identifiers from the sofa text.
     */
    private static final int SEED = -2009398202;

    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128(SEED);

    private static final Pattern TRAILING_WHITESPACE = Pattern.compile("\\s+$");

    private final Map<Integer, String> identifierForFsRef;

    private final Map<String, Collection<String>> childToParentMap;

    private final Map<String, FsDocumentLocation> documentLocationMap;

    private final CAS cas;

    private final String casIdentifier;

    private final String documentText;

    private final HashCode documentIdentifier;

    private final String casViewIdentifier;

    private final BlockingQueue<Integer> fsRefQueue;

    private final Predicate<String> typeFilter;

    SofaData(String casIdentifier,
             CAS cas,
             Predicate<String> typeFilter) {
        identifierForFsRef = new ConcurrentHashMap<>();
        childToParentMap = new ConcurrentHashMap<>();
        documentLocationMap = new ConcurrentHashMap<>();

        this.casIdentifier = casIdentifier;
        this.cas = cas;
        this.typeFilter = typeFilter;

        SofaFS sofa = cas.getSofa();
        String text;
        if (sofa == null || (text = sofa.getLocalStringData()) == null) {
            text = "";
        }
        documentText = TRAILING_WHITESPACE.matcher(text).replaceFirst("");

        documentIdentifier = HASH_FUNCTION.hashString(documentText, Charsets.UTF_8);

        casViewIdentifier = Strings.base64UUID();

        fsRefQueue = new LinkedBlockingQueue<>();
    }

    public String getDocumentText() {
        return documentText;
    }

    public HashCode getDocumentIdentifier() {
        return documentIdentifier;
    }

    public String getDocumentIdentifierString() {
        return documentIdentifier.toString();
    }

    public String getCasIdentifierString() {
        return casIdentifier;
    }

    public CAS getCas() {
        return cas;
    }

    public String getCasViewIdentifierString() {
        return casViewIdentifier;
    }

    public String getIdentifierForFs(FeatureStructure featureStructure) throws InterruptedException {
        int fsRef = featureStructure.getCAS().getLowLevelCAS().ll_getFSRef(featureStructure);

        Type type = featureStructure.getType();
        if (type == null) {
            throw new IllegalArgumentException("type was null");
        }
        String typeName = type.getName();

        String identifier = identifierForFsRef.get(fsRef);
        boolean newlyCreated = false;
        if (identifier == null) {
            synchronized (this) {
                identifier = identifierForFsRef.get(fsRef);
                if (identifier == null) {
                    identifier = Strings.base64UUID();
                    identifierForFsRef.put(fsRef, identifier);
                }
                newlyCreated = true;
            }
        }

        if (newlyCreated && typeFilter.apply(typeName)) {
            fsRefQueue.put(fsRef);
        }

        return identifier;
    }

    public void markAsChild(String childIdentifier, String parentIdentifier) {
        childToParentMap.compute(childIdentifier, (String k, @Nullable Collection<String> v) -> {
            Collection<String> parentUuids = v;
            if (parentUuids == null) {
                parentUuids = new ArrayList<>();
            }
            parentUuids.add(parentIdentifier);
            return parentUuids;
        });
    }

    public void addLocation(String featureStructureIdentifier, FsDocumentLocation fsDocumentLocation) {
        documentLocationMap.put(featureStructureIdentifier, fsDocumentLocation);
    }

    public Map<String, Collection<String>> getChildToParentMap() {
        return childToParentMap;
    }

    public Map<String, FsDocumentLocation> getDocumentLocationMap() {
        return documentLocationMap;
    }

    public BlockingQueue<Integer> getFsRefQueue() {
        return fsRefQueue;
    }
}

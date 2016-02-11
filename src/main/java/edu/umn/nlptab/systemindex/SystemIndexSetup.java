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

import edu.umn.nlptab.core.ElasticSearchSetup;
import edu.umn.nlptab.core.InstanceIndexes;
import edu.umn.nlptab.NlpTabException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.*;

/**
 * Responsible for initializing and the streams and settings used for a system index on NlpTab's ElasticSearch server.
 *
 * @author Ben Knoll
 * @since 1.0
 */
public class SystemIndexSetup {
    private static final ESLogger LOGGER = Loggers.getLogger(SystemIndexSetup.class);

    private final ElasticSearchSetup elasticSearchSetup;

    private final Client client;

    private String metadataIndex;

    private String primaryIndex;

    private String systemName;

    private String systemDescription;

    private List<String> ignoredViews;

    /**
     * Default constructor, usually instantiated through Guice.
     *
     * @param elasticSearchSetup the object for setting up generic ElasticSearch indexes and type streams.
     */
    @Inject
    SystemIndexSetup(Client client, ElasticSearchSetup elasticSearchSetup) {
        this.client = client;
        this.elasticSearchSetup = elasticSearchSetup;
    }

    @SuppressForbidden(reason = "toLowerCase is not used for localized text")
    public void initFromJsonMap(Map<String, Object> jsonObject) throws NlpTabException {
        String systemName = (String) jsonObject.get("systemName");
        if (systemName == null) {
            throw new NlpTabException("systemName was null");
        }
        this.systemName = systemName;

        String systemDescription = (String) jsonObject.get("systemDescription");
        if (systemDescription == null) {
            throw new NlpTabException("systemDescription was null");
        }
        this.systemDescription = systemDescription;

        List<String> ignoredViews = (List<String>) jsonObject.get("ignoredViews");
        if (ignoredViews == null) {
            ignoredViews = Collections.emptyList();
        }
        this.ignoredViews = ignoredViews;

        String instance = (String) jsonObject.get("instance");
        if (instance == null) {
            throw new NlpTabException("instance was null");
        }
        metadataIndex = InstanceIndexes.of(instance).metadataIndex();

        primaryIndex = systemName.replaceAll("\\W", "-").toLowerCase() + "-" + UUID.randomUUID().toString().toLowerCase();

        elasticSearchSetup.setIndex(primaryIndex);
        elasticSearchSetup.setInstance(instance);
    }

    /**
     * Initializes the primary index where data is stored about the CASes in this system. Also initializes the types on
     * the primary, metadata, and search indexes.
     *
     * @throws NlpTabException if the setup fails.
     */
    public void setupElasticSearch() throws NlpTabException {
        LOGGER.info("Initializing ElasticSearch indexes and type mappings");
        elasticSearchSetup.setUpPrimaryIndex("edu/umn/nlptab/systemindex/SystemIndexSettings.json");

        elasticSearchSetup.setUpPrimaryIndexType("FeatureStructure",
                "edu/umn/nlptab/systemindex/FeatureStructureMapping.json");

        elasticSearchSetup.setUpPrimaryIndexType("Type",
                "edu/umn/nlptab/systemindex/TypeMapping.json");

        elasticSearchSetup.setUpMetadataIndexType("SystemIndex",
                "edu/umn/nlptab/systemindex/MetadataSystemIndexMapping.json");

        elasticSearchSetup.setUpSearchIndexType("DocumentInSystem",
                "edu/umn/nlptab/systemindex/SearchDocumentInSystemMapping.json");

        elasticSearchSetup.setUpSearchIndexType("Document",
                "edu/umn/nlptab/systemindex/SearchDocumentMapping.json");
    }

    /**
     * Uploads the metadata index.
     *
     * @throws InterruptedException if the upload fails.
     */
    public void uploadMetadata() throws InterruptedException, IOException {
        client.prepareIndex(metadataIndex, "SystemIndex")
                .setId(primaryIndex)
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("index", primaryIndex)
                        .field("system", systemName)
                        .field("description", systemDescription)
                        .field("created", new Date())
                        .field("ready", false)
                        .field("ignoredViews", ignoredViews)
                        .endObject())
                .get().getId();
    }

    public String getPrimaryIndex() {
        return primaryIndex;
    }
}

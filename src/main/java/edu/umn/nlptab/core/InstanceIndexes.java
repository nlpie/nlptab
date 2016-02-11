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

package edu.umn.nlptab.core;

/**
 * Provides some default index names based on the instance name. In nlp-tab an instance is a method to separate sets of
 * systems and documents from one another. Each has its own indexes for search, system metadata, and analysis.
 *
 * @author Ben Knoll
 * @since 1.0
 */
public class InstanceIndexes {
    /**
     * The default suffix for the search index.
     */
    public static final String SEARCH_SUFFIX = "search";

    /**
     * The default suffix for the metadata index.
     */
    public static final String METADATA_SUFFIX = "metadata";

    /**
     * The default suffix for the analysis index.
     */
    public static final String ANALYSIS_SUFFIX = "analysis";

    /**
     * The instance name.
     */
    private final String instanceName;

    /**
     * Default constructor, takes the instance name as a parameter.
     *
     * @param instanceName the instance name.
     */
    public InstanceIndexes(String instanceName) {
        this.instanceName = instanceName;
    }

    public static InstanceIndexes of(String instanceName) {
        return new InstanceIndexes(instanceName);
    }

    /**
     * Retrieve the search index name for the current index.
     *
     * @return string index name
     */
    public String searchIndex() {
        return instanceName + SEARCH_SUFFIX;
    }

    /**
     * Returns the metadata index for the current index.
     *
     * @return string index name.
     */
    public String metadataIndex() {
        return instanceName + METADATA_SUFFIX;
    }

    /**
     * Returns the analysis index for the current index.
     *
     * @return string index name.
     */
    public String analysisIndex() {
        return instanceName + ANALYSIS_SUFFIX;
    }


    public String getInstanceName() {
        return instanceName;
    }
}

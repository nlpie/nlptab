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

import edu.umn.nlptab.core.InstanceIndexes;

/**
 *
 */
public class SystemIndexingSettings {
    private final InstanceIndexes instanceIndexes;

    private final String index;

    private final boolean useXCas;

    public SystemIndexingSettings(String instance, String index, boolean useXCas) {
        this.instanceIndexes = InstanceIndexes.of(instance);
        this.index = index;
        this.useXCas = useXCas;
    }

    public InstanceIndexes getInstanceIndexes() {
        return instanceIndexes;
    }

    public String getIndex() {
        return index;
    }

    public boolean useXCas() {
        return useXCas;
    }
}

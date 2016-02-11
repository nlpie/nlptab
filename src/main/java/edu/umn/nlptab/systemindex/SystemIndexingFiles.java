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

import edu.umn.nlptab.NlpTabException;
import org.apache.uima.UIMAFramework;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Iterator;

/**
 *
 */
public interface SystemIndexingFiles {
    /**
     * Returns the path for a type system descriptor.
     * @return path object which points to the location of the type system descriptor.
     * @throws IOException
     */
    Path getTypeSystemDescriptorPath() throws IOException;

    default TypeSystemDescription getTypeSystemDescription() throws NlpTabException {
        try {
            Path typeSystemDescriptorPath = getTypeSystemDescriptorPath();
            URL aURL = typeSystemDescriptorPath.toUri().toURL();
            XMLInputSource tsInputSource = new XMLInputSource(aURL);
            return UIMAFramework.getXMLParser().parseTypeSystemDescription(tsInputSource);
        } catch (InvalidXMLException | IOException e) {
            throw new NlpTabException("Failed to parse type system description", e);
        }
    }

    long getDocumentFileCount();

    Iterator<Path> getSystemIndexingDocumentFiles() throws IOException;
}

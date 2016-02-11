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

import org.apache.uima.cas.text.AnnotationFS;

/**
 * Represents a location of an annotation or feature structure in a document, which is a tuple of begin and end of text.
 * Annotations are directly
 *
 * @since 0.4.0
 */
public class FsDocumentLocation implements Comparable<FsDocumentLocation> {
    private final int begin;
    private final int end;

    /**
     * Default constructor. Initializes the begin and end of the annotation within a document.
     *
     * @param begin index of the first character in the document location.
     * @param end   index after the last character in the document location.
     */
    public FsDocumentLocation(int begin, int end) {
        this.begin = begin;
        this.end = end;
    }

    /**
     * Where the annotation begins.
     *
     * @return int index of the first character in the document location.
     */
    public int getBegin() {
        return begin;
    }

    /**
     * Where the annotation ends.
     *
     * @return int index of the last character in the document location.
     */
    public int getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FsDocumentLocation that = (FsDocumentLocation) o;

        if (begin != that.begin) return false;
        return end == that.end;

    }

    @Override
    public int hashCode() {
        int result = begin;
        result = 31 * result + end;
        return result;
    }

    @Override
    public int compareTo(FsDocumentLocation o) {
        int result = Integer.compare(begin, o.begin);
        if (result != 0) return result;
        return Integer.compare(end, o.end);
    }

    /**
     * Convenience factory constructor which creates a Document location from a uima {@link AnnotationFS}.
     *
     * @param annotationFS uima annotation
     * @return new document location.
     */
    public static FsDocumentLocation fromAnnotationFS(AnnotationFS annotationFS) {
        return new FsDocumentLocation(annotationFS.getBegin(), annotationFS.getEnd());
    }
}

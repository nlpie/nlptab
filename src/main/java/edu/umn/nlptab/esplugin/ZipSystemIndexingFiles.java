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

package edu.umn.nlptab.esplugin;

import edu.umn.nlptab.systemindex.SystemIndexingFiles;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 *
 */
public class ZipSystemIndexingFiles implements SystemIndexingFiles {

    private final String extension;

    private final Path rootDirectory;

    private final int count;

    ZipSystemIndexingFiles(Path zipPath, boolean useXCas) throws IOException {
        FileSystem zipFileSystem = FileSystems.newFileSystem(zipPath, ClassLoader.getSystemClassLoader());

        Iterator<Path> iterator = zipFileSystem.getRootDirectories().iterator();

        if (!iterator.hasNext()) {
            throw new IllegalStateException("Zip file system has leass than one root directory");
        }

        rootDirectory = iterator.next();

        if (iterator.hasNext()) {
            throw new IllegalStateException("Zip file system has more than one root directory.");
        }

        extension = useXCas ? ".xml" : ".xmi";


        int[] count = new int[]{0};

        Files.walkFileTree(rootDirectory, new ZipFileWalker(documentFiles(path -> count[0]++)));

        this.count = count[0];
    }

    @Override
    public Path getTypeSystemDescriptorPath() throws IOException {
        Path[] result = new Path[]{null};

        Files.walkFileTree(rootDirectory, new ZipFileWalker(path -> {
            if (path.endsWith("TypeSystem.xml")) {
                result[0] = path;
                return FileVisitResult.TERMINATE;
            }
            return FileVisitResult.CONTINUE;
        }));


        if (result[0] != null) {
            return result[0];
        } else {
            throw new FileNotFoundException("Zip file does not contain TypeSystem.xml file");
        }
    }

    @Override
    public long getDocumentFileCount() {
        return count;
    }

    @Override
    public Iterator<Path> getSystemIndexingDocumentFiles() throws IOException {
        ArrayList<Path> paths = new ArrayList<>(count);

        Files.walkFileTree(rootDirectory, new ZipFileWalker(documentFiles(paths::add)));

        return paths.iterator();
    }

    private Function<Path, FileVisitResult> documentFiles(Consumer<Path> consumer) {
        return path -> {
            if (!path.endsWith("TypeSystem.xml")) {
                if (path.getFileName().toString().endsWith(extension)) {
                    consumer.accept(path);
                }
            }
            return FileVisitResult.CONTINUE;
        };
    }

    private class ZipFileWalker extends SimpleFileVisitor<Path> {

        private final Function<Path, FileVisitResult> fileConsumer;

        public ZipFileWalker(Function<Path, FileVisitResult> fileConsumer) {
            this.fileConsumer = fileConsumer;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            Path fileName = dir.getFileName();
            if (fileName != null && fileName.toString().startsWith(".")) {
                return FileVisitResult.SKIP_SUBTREE;
            }

            return super.preVisitDirectory(dir, attrs);
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (!file.getFileName().toString().startsWith(".")) {
                return fileConsumer.apply(file);
            }
            return super.visitFile(file, attrs);
        }
    }
}

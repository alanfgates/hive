/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.test.inmemfs;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class InMemoryFs extends FileSystem {

  private static InMemoryFile root;
  private static Map<Path, InMemoryFile> files;

  // Note that this can be null.  Access to this should be synchronized on 'this'
  private InMemoryFile cwd;

  public InMemoryFs() {
    synchronized (InMemoryFs.class) {
      if (root == null) {
        files = new HashMap<>();
        FsPermission perms = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
        root = new InMemoryDirectory(new Path("/"), perms, "root", "root");
        files.put(root.getPath(), root);
      }
    }
    setConf(new Configuration());
  }


  @Override
  public URI getUri() {
    try {
      return new URI("hiveinmemfs:///");
    } catch (URISyntaxException e) {
      throw new RuntimeException("Help me Obi Wan Kenobi", e);
    }
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    Path absolutePath = makePathAbsolute(path);
    InMemoryFile target;
    synchronized (InMemoryFs.class) {
      target = files.get(absolutePath);
      if (target == null) {
        throw new FileNotFoundException(path.toString() + ", no such file");
      }
      InMemoryRegularFile file = resolveSymLink(target).asRegularFile();
      checkCanRead(target);
      return new FSDataInputStream(new InputStreamWrapper(file.readData()));
    }
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
                                   int bufferSize, short replication,
                                   long blockSize, Progressable progressable) throws IOException {
    Path absolutePath = makePathAbsolute(path);
    Path parent = absolutePath.getParent();
    InMemoryRegularFile toWriteTo;
    synchronized (InMemoryFs.class) {
      InMemoryFile target = files.get(absolutePath);
      if (target != null) {
        if (!overwrite) throw new IOException(path.toString() + " already exists");
        toWriteTo = target.asRegularFile();

      } else {
        InMemoryFile parentFile = files.get(parent);
        if (parentFile == null) {
          throw new FileNotFoundException(parent.toString() + ", no such directory");
        }
        InMemoryDirectory parentDir = resolveSymLink(parentFile).asDirectory();
        checkCanCreateIn(parentDir);
        toWriteTo = new InMemoryRegularFile(absolutePath, fsPermission, getOwner(), getGroup());
        parentDir.addFile(absolutePath.getName(), toWriteTo);
        files.put(absolutePath, toWriteTo);
      }
    }
    return new FSDataOutputStream(new OutputStreamWrapper(toWriteTo, false), statistics, 0);
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    Path absolutePath = makePathAbsolute(path);
    InMemoryRegularFile toAppendTo;
    synchronized (InMemoryFs.class) {
      InMemoryFile target = files.get(absolutePath);
      if (target == null) {
        throw new FileNotFoundException(path.toString() + ", no such file");
      }
      toAppendTo = resolveSymLink(target).asRegularFile();
    }
    checkCanWrite(toAppendTo);
    return new FSDataOutputStream(new OutputStreamWrapper(toAppendTo, true), statistics,
        toAppendTo.getLen());
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    return false;
    // TODO
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    Path absolutePath = makePathAbsolute(path);
    if (path.toString().equals("/")) {
      throw new IOException("No no no!");
    }
    Path parentPath = absolutePath.getParent();
    synchronized (InMemoryFs.class) {
      InMemoryFile file = files.get(absolutePath);
      // This will NPE if we have a dangling file, but that's a bad situation anyway.
      InMemoryDirectory parentDir = files.get(parentPath).asDirectory();
      if (file == null) return false;
      if (file.stat().isDirectory()) {
        if (recursive) {
          innerDelete(parentDir, file);
        } else {
          throw new IOException(path.toString() + " is a directory");
        }
      } else {
        innerDelete(parentDir, file);
      }
    }
    return true;
  }

  // Assumes you hold the class lock
  private void innerDelete(InMemoryDirectory parent, InMemoryFile file) throws IOException {
    if (file.stat().isDirectory()) {
      // Have to make a copy of the files because we're going to delete them as we go
      List<InMemoryFile> contents = new ArrayList<>(file.asDirectory().getFiles());
      for (InMemoryFile f : contents) {
        innerDelete(file.asDirectory(), f);
      }
    }
    parent.removeFile(file.getPath().getName());
    files.remove(file.getPath());
  }

  @Override
  public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
    InMemoryFile file;
    Collection<InMemoryFile> dirFiles;
    synchronized (InMemoryFs.class) {
      file = files.get(path);
      if (file == null) {
        throw new FileNotFoundException(path.toString() + ", no such directory");
      }
      file = resolveSymLink(file);
      InMemoryDirectory dir = file.asDirectory();
      dirFiles = dir.getFiles();
    }
    List<FileStatus> stats = new ArrayList<>(dirFiles.size());
    for (InMemoryFile f : dirFiles) stats.add(f.stat());
    return stats.toArray(new FileStatus[dirFiles.size()]);
  }

  @Override
  public void setWorkingDirectory(Path path) {
    synchronized (this) {
      cwd = files.get(path);
    }
  }

  @Override
  public Path getWorkingDirectory() {
    synchronized (this) {
      return cwd == null ? null : cwd.getPath();
    }
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    path = makePathAbsolute(path);

    Stack<Path> pathsToCreate = new Stack<>();
    Path currPath = path;
    synchronized (InMemoryFs.class) {
      if (files.containsKey(path)) return false;

      while (!files.containsKey(currPath)) {
        LOG.debug("Found path we need to create " + currPath.toString() + " with parent " +
            currPath.getParent().toString());
        pathsToCreate.add(currPath);
        currPath = currPath.getParent();
        assert currPath != null;
      }

      InMemoryFile currFile = resolveSymLink(files.get(currPath));

      // The end point better be a directory
      if (!currFile.stat().isDirectory()) {
        throw new IOException("Attempt to create a file in " + currPath +
            " which isn't a directory");
      }

      while (!pathsToCreate.empty()) {
        Path toCreate = pathsToCreate.pop();
        LOG.debug("Going to create path " + toCreate.toString());
        checkCanCreateIn(currFile.asDirectory());
        InMemoryDirectory newDir =
            new InMemoryDirectory(toCreate, fsPermission, getOwner(), getGroup());
        currFile.asDirectory().addFile(toCreate.getName(), newDir);
        currFile = newDir;
        assert toCreate.isAbsolute();
        files.put(toCreate, currFile);
      }

      return true;
    }
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    Path absolutePath = makePathAbsolute(path);
    InMemoryFile file;
    synchronized (InMemoryFs.class) {
      file = files.get(absolutePath);
    }
    if (file == null) {
      throw new FileNotFoundException(path.toString() + ", no such file or directory");
    }
    return file.stat();
  }

  private void checkCanCreateIn(InMemoryDirectory dir) throws IOException {
    checkCan(dir.stat(), FsAction.EXECUTE, "create in");
  }

  private void checkCanRead(InMemoryFile file) throws IOException {
    checkCan(file.stat(), FsAction.READ, "read");
  }

  private void checkCanWrite(InMemoryFile file) throws IOException {
    checkCan(file.stat(), FsAction.WRITE, "write");
  }

  private void checkCan(FileStatus stat, FsAction action, String verb) throws IOException {
    if (stat.getOwner().equals(getOwner())) {
      if (stat.getPermission().getUserAction().and(action) != action) {
        throw new IOException("User " + getOwner() + " does not have permission to " + verb +
            " " + stat.getPath().toString());
      }
      return;
    }

    for (String group : getGroups()) {
      if (stat.getGroup().equals(group)) {
        if (stat.getPermission().getGroupAction().and(action) != action) {
          throw new IOException("Group " + getGroup() + " does not have permission to " +
              verb + " " + stat.getPath().toString());
        }
        return;
      }
    }

    if (stat.getPermission().getOtherAction().and(action) != action) {
      throw new IOException("World does not have permission to " + verb +
          " " + stat.getPath().toString());
    }

  }

  private String getOwner() throws IOException {
    return UserGroupInformation.getCurrentUser().getUserName();
  }

  private String getGroup() throws IOException {
    return UserGroupInformation.getCurrentUser().getPrimaryGroupName();
  }

  private String[] getGroups() throws IOException {
    return UserGroupInformation.getCurrentUser().getGroupNames();
  }

  // This assumes you are not holding the class level lock
  private Path makePathAbsolute(Path path) throws IOException {
    if (path.isAbsolute()) {
      return path;
    } else {
      synchronized (this) {
        if (cwd == null) {
          throw new IOException("No current working directory, cannot use relative path");
        }
        return new Path(cwd.getPath(), path);
      }
    }
  }

  // This assumes you are holding the class level lock
  private InMemoryFile resolveSymLink(InMemoryFile file) throws IOException {
    if (file instanceof InMemorySymLink) {
      checkCanRead(file);
      InMemorySymLink symLink = file.asSymLink();
      InMemoryFile target = symLink.getLinkTo();
      // We need to check that the link target is still valid.
      if (!files.values().contains(target)) {
        throw new FileNotFoundException("Target of link " + symLink.getPath().toString() +
            " " + symLink.getLinkTo().getPath().toString() + " does not exist");
      }
      return resolveSymLink(target);
    } else {
      return file;
    }
  }

  @VisibleForTesting
  Map<Path, InMemoryFile> getFiles() {
    return files;
  }

  @VisibleForTesting
  static void reset() {
    files = null;
    root = null;
  }
}

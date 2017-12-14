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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class InMemoryDirectory extends InMemoryFile {

  private Map<String, InMemoryFile> files;

  InMemoryDirectory(Path path, FsPermission perms, String owner, String group) {
    super(path, perms, owner, group);
    files = new HashMap<>();
  }

  @Override
  FileStatus stat() {
    return new FileStatus(0, true, 1, 1, getLastModificationTime(), getLastAccessTime(), getPerms(),
        getOwner(), getGroup(), null, getPath());
  }

  void addFile(String name, InMemoryFile file) throws IOException {
    if (files.containsKey(name)) {
      throw new IOException("File " + name + " already exists");
    }
    files.put(name, file);
  }

  void removeFile(String name) {
    files.remove(name);
  }

  @Override
  InMemoryDirectory asDirectory() throws IOException {
    return this;
  }

  InMemoryFile getFile(String name) {
    return files.get(name);
  }

  Collection<InMemoryFile> getFiles() {
    return files.values();
  }
}

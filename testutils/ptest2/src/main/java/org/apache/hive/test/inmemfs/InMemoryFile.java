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

abstract class InMemoryFile {

  private InMemoryDirectory parent;
  private Path path;
  private long lastModificationTime;
  private long lastAccessTime;
  private FsPermission perms;
  private String owner;
  private String group;


  InMemoryFile(InMemoryDirectory parent, Path path, FsPermission perms, String owner,
               String group) throws IOException {
    this.parent = parent;
    if (parent != null) parent.addFile(path.getName(), this);
    this.path = path;
    assert path.isAbsolute();
    lastModificationTime = lastAccessTime = System.currentTimeMillis();
    this.perms = perms;
    this.owner = owner;
    this.group = group;
  }

  abstract FileStatus stat();

  InMemoryDirectory asDirectory() throws IOException {
    throw new IOException(path.toString() + " is not a directory");
  }

  InMemorySymLink asSymLink() throws IOException {
    throw new IOException(path.toString() + " is not a symlink");
  }

  InMemoryRegularFile asRegularFile() throws IOException {
    throw new IOException(path.toString() + " is not a regular file");
  }

  Path getPath() {
    return path;
  }

  void move(Path newPath, InMemoryDirectory newParent) throws IOException {
    parent.removeFile(this.getPath().getName());
    this.path = newPath;
    this.parent = newParent;
    parent.addFile(this.getPath().getName(), this);
    updateModificationTime();
  }

  void setPath(Path newPath) {
    this.path = newPath;
    updateModificationTime();
  }

  long getLastModificationTime() {
    return lastModificationTime;
  }

  long getLastAccessTime() {
    return lastAccessTime;
  }

  FsPermission getPerms() {
    return perms;
  }

  String getOwner() {
    return owner;
  }

  String getGroup() {
    return group;
  }

  void chmod(FsPermission perms) {
    this.perms = perms;
    updateModificationTime();
  }

  void chown(String owner) {
    this.owner = owner;
    updateModificationTime();
  }

  void chgrp(String group) {
    this.group = group;
    updateModificationTime();
  }

  void updateModificationTime() {
    lastModificationTime = System.currentTimeMillis();
  }
}

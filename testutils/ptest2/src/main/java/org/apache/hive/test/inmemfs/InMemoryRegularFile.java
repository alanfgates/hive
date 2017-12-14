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
import java.util.Arrays;

class InMemoryRegularFile extends InMemoryFile {
  private byte[] data;

  InMemoryRegularFile(Path path, FsPermission perms, String owner, String group) {
    super(path, perms, owner, group);
  }

  @Override
  FileStatus stat() {
    return new FileStatus(getLen(), false, 1, getLen(), getLastModificationTime(),
        getLastAccessTime(), getPerms(), getOwner(), getGroup(), null, getPath());
  }

  void writeData(byte[] input, boolean append) throws IOException {
    if (data != null && append) {
      byte[] old = data;
      data = new byte[old.length + input.length];
      System.arraycopy(old, 0, data, 0, old.length);
      System.arraycopy(input, 0, data, old.length, input.length);
    } else {
      data = Arrays.copyOf(input, input.length);
    }
  }

  byte[] readData() {
    return data;
  }

  long getLen() {
    return data == null ? 0 : data.length;
  }

  @Override
  InMemoryRegularFile asRegularFile() throws IOException {
    return this;
  }
}

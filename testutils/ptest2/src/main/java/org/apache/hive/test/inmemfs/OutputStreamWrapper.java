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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class OutputStreamWrapper extends ByteArrayOutputStream {
  private final InMemoryRegularFile file;
  private final boolean append;

  public OutputStreamWrapper(InMemoryRegularFile file, boolean append) {
    this.file = file;
    this.append = append;
  }

  @Override
  public void close() throws IOException {
    super.close();
    file.writeData(super.toByteArray(), append);
    file.updateModificationTime();
  }
}

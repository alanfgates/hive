# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

THISSERVICE=capygen
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

capygen () {
  CLASS=org.apache.hive.test.capybara.tools.UserQueryGenerator
  HIVE_OPTS=''
  execHiveCmd $CLASS "$@"
}

capygen_help () {
  echo "usage ./hive capygen [-h] -i input_file ... -o output_file"
  echo ""
  echo "  --input (-i)                Input files to read queries from.  Eache file becomes a separate JUnit test."
  echo "  --output (-o)               Output file to write tests to.  All tests will be placed in one class"
  echo "  --help (-h)                 Print help message"
} 

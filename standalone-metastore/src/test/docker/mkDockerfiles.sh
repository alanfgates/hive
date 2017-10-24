#!/bin/bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script assumes that it is invoked from the standalone-metastore directory
# If not, it dies

if [ `basename $PWD` != "standalone-metastore" ] 
then
  echo "You must invoke this from the standalone-metastore directory.  Current directory is $PWD"
  exit 1
fi

TARBALL=`ls $PWD/target/apache-hive-metastore-*-bin.tar.gz`
len=${#TARBALL}
UNTARRED=${TARBALL:0:$len-7}

if [ ! -f $TARBALL ] 
  then
  echo "$TARBALL : no such file or directory"
  echo "You must build the binary tarball before running this script.  Do 'mvn package'"
  exit 1
fi

rm -rf target/docker
mkdir target/docker

tarball_filename=`basename $TARBALL`
untarred_dir=`basename $UNTARRED`
for dir in postgres mysql
do
  docker_src_dir="$PWD/src/test/docker/$dir"
  docker_target_dir="$PWD/target/docker/$dir"
  mkdir $docker_target_dir
  cp $TARBALL $docker_target_dir
  cp $docker_src_dir/metastore-site.xml $docker_target_dir
  sed -e s@__REPLACE_ME_WITH_TARBALL_NAME__@$tarball_filename@ \
    -e s@__REPLACE_ME_WITH_UNTARRED_NAME__@$untarred_dir@ \
    $docker_src_dir/Dockerfile.proto > $docker_target_dir/Dockerfile
done


# Licensed to the vertx-pipes project under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env bash

set -e

if [ -z "${OLD_VERSION}" ]; then
    OLD_VERSION=`git tag | tail -1 | sed 's/.*-//g'`
fi

if [ -z "${NEW_VERSION}" ]; then
    NEW_VERSION=`python -c "print $OLD_VERSION+0.1"`
fi

mvn -Darguments="-Dmaven.test.skip=true -Dgpg.passphrase=${GPG_PASSWORD}" release:prepare release:perform

sed -i -e "s/${OLD_VERSION}/${NEW_VERSION}/g" readme.md
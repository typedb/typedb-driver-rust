#
# Copyright (C) 2022 Vaticle
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def vaticle_dependencies():
#    git_repository(
#        name = "vaticle_dependencies",
#        remote = "https://github.com/dmitrii-ubskii/dependencies",
#        branch = "api-rearchitecture",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_dependencies
#    )
    native.local_repository(
        name = "vaticle_dependencies",
        path = "../dependencies",
    )

def vaticle_typedb_common():
    git_repository(
        name = "vaticle_typedb_common",
        remote = "https://github.com/vaticle/typedb-common",
        commit = "aa03cb5f6a57ec2a51291b7a0510734ca1f41479" # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_common
    )

def vaticle_typedb_protocol():
    git_repository(
        name = "vaticle_typedb_protocol",
        remote = "https://github.com/dmitrii-ubskii/typedb-protocol",
        branch = "api-rearchitecture",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_protocol
    )

def vaticle_typeql():
    git_repository(
        name = "vaticle_typeql",
        remote = "https://github.com/vaticle/typeql",
        commit = "7a63699b3879296ae3039577ba3f5220bbf6d33d", # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_dependencies
    )

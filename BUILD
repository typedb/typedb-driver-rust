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

package(default_visibility = ["//visibility:public"])

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_static_library", "rustfmt_test")
load("@vaticle_bazel_distribution//crates:rules.bzl", "assemble_crate", "deploy_crate")
load("@vaticle_bazel_distribution//github:rules.bzl", "deploy_github")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("//:deployment.bzl", deployment_github = "deployment")
load("@vaticle_dependencies//builder/rust:rules.bzl", "rust_cbindgen")
load("@vaticle_dependencies//builder/swig:java.bzl", "swig_java")

typedb_client_srcs = glob(["src/**/*.rs"])
typedb_client_tags = ["crate-name=typedb-client"]
typedb_client_deps = [
        "@crates//:chrono",
        "@crates//:crossbeam",
        "@crates//:futures",
        "@crates//:http",
        "@crates//:itertools",
        "@crates//:log",
        "@crates//:prost",
        "@crates//:tokio",
        "@crates//:tokio-stream",
        "@crates//:tonic",
        "@crates//:uuid",
        "@vaticle_typedb_protocol//grpc/rust:typedb_protocol",
        "@vaticle_typeql//rust:typeql_lang",
    ]
typedb_client_proc_macro_deps = [
    "@crates//:async-trait",
    "@crates//:maybe-async",
]

rust_library(
    name = "typedb_client",
    srcs = typedb_client_srcs,
    deps = typedb_client_deps,
    proc_macro_deps = typedb_client_proc_macro_deps,
    tags = typedb_client_tags,
)

rust_static_library(
    name = "typedb_client_clib",
    srcs = typedb_client_srcs,
    deps = typedb_client_deps,
    proc_macro_deps = typedb_client_proc_macro_deps,
    tags = typedb_client_tags,
    crate_features = ["sync"],
)

rust_cbindgen(
    name = "typedb_client_h",
    lib = ":typedb_client_clib",
    header_name = "typedb_client.h",
    config = "cbindgen.toml",
)

swig_java(
    name = "typedb_client_jni",
    lib = ":typedb_client_h",
    package = "com.vaticle.typedb.client.jni",
    interface = "typedb_client.i",
    includes = ["swig/typedb_client_java.swg"],
    enable_cxx = True,
)

assemble_crate(
    name = "assemble_crate",
    description = "TypeDB Client API for Rust",
    homepage = "https://github.com/vaticle/typedb-client-rust",
    license = "Apache-2.0",
    repository = "https://github.com/vaticle/typedb-client-rust",
    target = "typedb_client",
)

deploy_crate(
    name = "deploy_crate",
    release = deployment["crate.release"],
    snapshot = deployment["crate.snapshot"],
    target = ":assemble_crate",
)

deploy_github(
    name = "deploy_github",
    draft = True,
    organisation = deployment_github["github.organisation"],
    release_description = "//:RELEASE_TEMPLATE.md",
    repository = deployment_github["github.repository"],
    title = "TypeDB Client Rust",
    title_append_version = True,
)

checkstyle_test(
    name = "checkstyle",
    size = "small",
    include = glob([
        "*",
        "src/**/*",
        "tools/*",
        ".factory/*",
    ]),
    exclude = glob([
        "*.md",
        ".bazelversion",
        ".bazel-remote-cache.rc",
        ".bazel-cache-credential.json",
        "LICENSE",
        "VERSION",
    ]),
    license_type = "apache-header",
)

checkstyle_test(
    name = "checkstyle-license",
    size = "small",
    include = ["LICENSE"],
    license_type = "apache-fulltext",
)

filegroup(
    name = "rustfmt_config",
    srcs = ["rustfmt.toml"],
)

rustfmt_test(
    name = "client_rustfmt_test",
    targets = ["typedb_client"],
)

# CI targets that are not declared in any BUILD file, but are called externally
filegroup(
    name = "ci",
    data = [
        "@vaticle_dependencies//tool/bazelinstall:remote_cache_setup.sh",
        "@vaticle_dependencies//tool/ide:rust_sync",
    ],
)

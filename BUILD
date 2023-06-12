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
load("@vaticle_dependencies//tool/swig:rules.bzl", "swig_java")

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

rust_library(
    name = "typedb_client",
    srcs = typedb_client_srcs,
    tags = typedb_client_tags,
    deps = typedb_client_deps,
    proc_macro_deps = [
        "@crates//:async-trait",
    ]
)

rust_static_library(
    name = "typedb_client_so",
    srcs = typedb_client_srcs,
    tags = typedb_client_tags,
    deps = typedb_client_deps,
    proc_macro_deps = [
        "@crates//:async-trait",
    ]
)

rust_cbindgen(
    name = "typedb_client_h",
    lib = ":typedb_client_so",
    header_name = "typedb_client.h",
)

swig_java(
    name = "typedb_client_java",
    lib = ":typedb_client_h",
    package = "com.vaticle.typedb.client.jni",
)

cc_binary(
    name = "libtypedb_client_java_jni.dylib",
    linkopts = ["-Wl,-install_name,libtypedb_client_java_jni.dylib"],
    linkshared = True,
    deps = [":typedb_client_java", ":typedb_client_h"],
    srcs = [":typedb_client_java"],
)

java_binary(
    name = "jni-test",
    srcs = ["Main.java"],
    main_class = "Main",
    deps = [":libtypedb_client_java_jni.dylib"],
    #jvm_flags = ["-Djava.library.path=/private/var/tmp/_bazel_dmitriiubskii/ac34f3129c829d86eb8439970c02f532/execroot/__main__/bazel-out/darwin_arm64-fastbuild/bin"],
    #jvm_flags = ["-Djava.library.path=bazel-bin"],
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
        "@vaticle_dependencies//tool/cargo:sync",
    ],
)

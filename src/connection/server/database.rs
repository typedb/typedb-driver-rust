/*
 * Copyright (C) 2022 Vaticle
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::fmt::{Display, Formatter};

use crate::common::{Connection, Result};

#[derive(Clone, Debug)]
pub struct Database {
    name: String,
    connection: Connection,
}

impl Database {
    pub(crate) fn new(name: String, connection: Connection) -> Self {
        Database { name, connection }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub async fn delete(self) -> Result {
        self.connection.delete_database(self.name).await
    }

    pub async fn schema(&self) -> Result<String> {
        self.connection.database_schema(self.name.clone()).await
    }

    pub async fn type_schema(&self) -> Result<String> {
        self.connection.database_type_schema(self.name.clone()).await
    }

    pub async fn rule_schema(&self) -> Result<String> {
        self.connection.database_rule_schema(self.name.clone()).await
    }
}

impl Display for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

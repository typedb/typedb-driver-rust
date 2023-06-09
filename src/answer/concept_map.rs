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

use std::{
    collections::{hash_map, HashMap},
    ops::Index,
};

use crate::concept::Concept;

#[derive(Debug, PartialEq)]
pub struct ConceptMap {
    pub map: HashMap<String, Concept>,
    pub explainables: Option<Explainables>,
}

#[derive(Debug, PartialEq)]
pub struct Explainables {
    pub relations: HashMap<String, Explainable>,
    pub attributes: HashMap<String, Explainable>,
    pub ownerships: HashMap<(String, String), Explainable>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Explainable {
    pub conjunction: String,
    pub id: i64,
}

impl ConceptMap {
    pub fn get(&self, var_name: &str) -> Option<&Concept> {
        self.map.get(var_name)
    }

    pub fn concepts(&self) -> impl Iterator<Item = &Concept> {
        self.map.values()
    }

    pub fn concepts_to_vec(&self) -> Vec<&Concept> {
        self.concepts().collect::<Vec<&Concept>>()
    }
}

impl Clone for ConceptMap {
    fn clone(&self) -> Self {
        let mut map = HashMap::with_capacity(self.map.len());
        for (k, v) in &self.map {
            map.insert(k.clone(), v.clone());
        }
        Self { map, explainables: self.explainables.clone() }
    }
}

impl From<ConceptMap> for HashMap<String, Concept> {
    fn from(cm: ConceptMap) -> Self {
        cm.map
    }
}

impl Index<String> for ConceptMap {
    type Output = Concept;

    fn index(&self, index: String) -> &Self::Output {
        &self.map[&index]
    }
}

impl IntoIterator for ConceptMap {
    type Item = (String, Concept);
    type IntoIter = hash_map::IntoIter<String, Concept>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

impl Explainables {
    pub(crate) fn new(
        relations: HashMap<String, Explainable>,
        attributes: HashMap<String, Explainable>,
        ownerships: HashMap<(String, String), Explainable>,
    ) -> Self {
        Self { relations, attributes, ownerships }
    }
}

impl Explainable {
    pub(crate) fn new(conjunction: String, id: i64) -> Self {
        Self { conjunction, id }
    }
}

impl Clone for Explainables {
    fn clone(&self) -> Self {
        let mut relations = HashMap::with_capacity(self.relations.len());
        for (k, v) in &self.relations {
            relations.insert(k.clone(), v.clone());
        }
        let mut attributes = HashMap::with_capacity(self.attributes.len());
        for (k, v) in &self.attributes {
            attributes.insert(k.clone(), v.clone());
        }
        let mut ownerships = HashMap::with_capacity(self.ownerships.len());
        for (k, v) in &self.ownerships {
            ownerships.insert(k.clone(), v.clone());
        }

        Self { relations, attributes, ownerships }
    }
}

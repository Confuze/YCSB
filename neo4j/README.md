<!--
Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on neo4j.

It is an up-to-date implementation inspired by an older project - https://github.com/ysnglt/YCSB-neo4j

### 1. Start and configure neo4j

This driver will connect to a running neo4j databse under your provided url.

The driver can work without any schema but the record key should be indexed. To achieve this, please create a unique constraint on the \_key property for nodes with the usertable label. (or whichever table your chosen workload uses) Here is an example cypher query to do this.

    CREATE CONSTRAINT key_constraint FOR (n:usertable) REQUIRE n._key IS NODE KEY

You can run it after the database has started with the cypher shell, or you can use a cypher init script with (apoc)[https://neo4j.com/labs/apoc/4.2/operational/init-script/].

### 2. Set Up YCSB

You need to clone the repository and compile everything.

    git clone git://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn clean package

Or compile just the neo4j bidning.

    mvn -pl site.ycsb:neo4j-binding -am clean package

### 3. Run YCSB

First, load the data:

    ./bin/ycsb load neo4j -s -P workloads/workloada -p db.path=path/to/database

Then, run the workload:

    ./bin/ycsb run neo4j -s -P workloads/workloada -p db.path=path/to/database

## neo4j Configuration Parameters

todo

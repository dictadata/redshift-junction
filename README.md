# @dictadata/redshift-junction

A storage junction for Amazon Redshift database access using ODBC.

## Installation

Requires Redshift ODBC drivers.

Requires node-gyp and a C++ compiler to build the NPM odbc package.

## Junction Plugin

Register the junction when initializing the app.

```javascipt
const storage = require("@dictadata/storage-junctions");

const RedshiftJunction = require("@dictadata/redshift-junction");
storage.use("redshift", RedshiftJunction);
```

Then a junction can be created elsewhere in the app using a SMT.

```
const storage = require("@dictadata/storage-junctions");

var junction = storage.activate({
  smt: {
    model:"redshift",
    locus: "DSN=xyz",
    schema: "table",
    key: "*"
  }
});
```

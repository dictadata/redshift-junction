/**
 * test/redshift-junction
 */
"use strict";

const storage = require("@dictadata/storage-junctions");
const RedshiftJunction = require("../lib/redshift");
const logger = require('./logger');

const stream = require('stream/promises');


console.log("=== Tests: RedshiftJunction");

console.log("--- adding RedshiftJunction to storage cortex");
storage.use("redshift", RedshiftJunction);


async function testStream() {
  console.log("=== testStream");

  console.log(">>> create junction");
  var junction = storage.activate({
    smt: {
      model: "redshift",
      locus: "somewhere",
      schema: "container",
      key: "*"
    }
  }, {
    logger: logger
  });

  console.log(">>> create streams");
  var reader = junction.getReadStream({});
  var writer = junction.getWriteStream({});

  //console.log(">>> start pipe");
  //await stream.pipeline(reader,writer);

  await junction.relax();

  console.log(">>> completed");
}

async function tests() {
  await testStream();
}

tests();

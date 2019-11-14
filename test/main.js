/**
 * test/redshift-junction
 */
"use strict";

const storage = require("@dictadata/storage-junctions");
const RedshiftJunction = require("../lib/redshift");

const stream = require('stream');
const util = require('util');

const pipeline = util.promisify(stream.pipeline);

console.log("=== Tests: RedshiftJunction");


async function testStream() {
  console.log("=== testStream");

  console.log(">>> adding RedshiftJunction to storage cortex");
  storage.use("redshift", RedshiftJunction);

  console.log(">>> create junction");
  var junction = storage.activate({
    smt: {
      model:"redshift",
      locus: "somewhere",
      schema: "container",
      key: "*"
    }
  });

  console.log(">>> create streams");
  var reader = junction.getReadStream({});
  var writer = junction.getWriteStream({});

  //console.log(">>> start pipe");
  //await pipeline(reader,writer);

  await junction.relax();

  console.log(">>> completed");
}

async function tests() {
  await testStream();
}

tests();

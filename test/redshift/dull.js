/**
 * test/redshift/dull
 */
"use strict";

const storage = require("@dictadata/storage-junctions");
const { logger } = require("@dictadata/storage-junctions").utils;
const { dull } = require("@dictadata/storage-junctions").test;
const RedshiftJunction = require("../../lib/redshift");

logger.info("=== Test: redshift");

console.log("--- adding RedshiftJunction to storage cortex");
storage.use("redshift", RedshiftJunction);


async function tests() {

  logger.info("=== redshift dull");
  await dull({
    source: {
      smt: "redshift|DSN=drewlab|foo_schema|*",
      pattern: {
        match: {
          Foo: 'twenty'
        }
      },
      options: {
        logger: logger
      }
    }
  });

}

(() => {
  await tests();
})();

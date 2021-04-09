/**
 * test/redshift/recall
 */
"use strict";

const storage = require("@dictadata/storage-junctions");
const { logger } = require("@dictadata/storage-junctions").utils;
const { recall } = require("@dictadata/storage-junctions").test;
const RedshiftJunction = require("../../lib/redshift");

logger.info("=== Test: redshift");

console.log("--- adding RedshiftJunction to storage cortex");
storage.use("redshift", RedshiftJunction);


async function tests() {

  logger.info("=== redshift recall");
  await recall({
    source: {
      smt: "redshift|DSN=drewlab|foo_schema|=Foo",
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

  logger.info("=== redshift recall");
  await recall({
    source: {
      smt: "redshift|DSN=drewlab|foo_schema|=Foo",
      pattern: {
        match: {
          Foo: 'ten'
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

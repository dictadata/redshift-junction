/**
 * test/redshift/retrieve
 */
"use strict";

const storage = require("@dictadata/storage-junctions");
const { logger } = require("@dictadata/storage-junctions").utils;
const { retrieve } = require("@dictadata/storage-junctions").test;
const RedshiftJunction = require("../../lib/redshift");

logger.info("=== Test: redshift");

console.log("--- adding RedshiftJunction to storage cortex");
storage.use("redshift", RedshiftJunction);


async function tests() {

  logger.info("=== redshift retrieve");
  await retrieve({
    source: {
      smt: "redshift|DSN=drewlab|foo_schema|*",
      pattern: {
        match: {
          "Foo": 'twenty'
        }
      },
      options: {
        logger: logger
      }
    }
  });

  logger.info("=== redshift retrieve with pattern");
  await retrieve({
    source: {
      smt: "redshift|DSN=drewlab|foo_transfer|*",
      pattern: {
        match: {
          "Foo": "first",
          "Baz": { "gte": 0, "lte": 1000 }
        },
        cues: {
          count: 3,
          order: { "Dt Test": "asc" },
          fields: ["Foo","Baz"]
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

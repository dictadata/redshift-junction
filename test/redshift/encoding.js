/**
 * test/redshift/encoding
 */
"use strict";

const storage = require("@dictadata/storage-junctions");
const { logger } = require("@dictadata/storage-junctions").utils;
const { getEncoding, createSchema } = require("@dictadata/storage-junctions").test;
const RedshiftJunction = require("../../lib/redshift");

logger.info("=== Test: redshift");

console.log("--- adding RedshiftJunction to storage cortex");
storage.use("redshift", RedshiftJunction);


async function tests() {

  logger.info("=== redshift putEncoding");
  await createSchema({
    source: {
      smt: "redshift|DSN=drewlab|foo_schema|*",
      options: {
        logger: logger
      }
    }
  });

  logger.info("=== redshift getEncoding");
  await getEncoding({
    source: {
      smt: "redshift|DSN=drewlab|foo_schema|*",
      options: {
        logger: logger
      }
    },
    OutputFile: './test/output/redshift_foo_encoding.json'
  });

}

(() => {
  await tests();
})();

/**
 * test/redshift/store
 */
"use strict";

const storage = require("@dictadata/storage-junctions");
const { logger } = require("@dictadata/storage-junctions").utils;
const { store } = require("@dictadata/storage-junctions").test;
const RedshiftJunction = require("../../lib/redshift");

logger.info("=== Test: redshift");

console.log("--- adding RedshiftJunction to storage cortex");
storage.use("redshift", RedshiftJunction);


async function tests() {

  logger.info("=== redshift store 20");
  await store({
    source: {
      smt: "redshift|DSN=drewlab|foo_schema|=Foo",
      options: {
        logger: logger
      }
    },
    construct: {
      Foo: 'twenty',
      Bar: 'Jackson',
      Baz: 20
    }
  });

  logger.info("=== redshift store 30");
  await store({
    source: {
      smt: "redshift|DSN=drewlab|foo_schema|=Foo",
      options: {
        logger: logger
      }
    },
    construct: {
      Foo: 'twenty',
      Bar: 'Jackson',
      Baz: 30,
      enabled: false
    }
  });

  logger.info("=== redshift store 10");
  await store({
    source: {
      smt: "redshift|DSN=drewlab|foo_schema|=Foo",
      options: {
        logger: logger
      }
    },
    construct: {
      Foo: 'ten',
      Bar: 'Hamilton',
      Baz: 10,
      enabled: false
    }
  });

}

(() => {
  await tests();
})();

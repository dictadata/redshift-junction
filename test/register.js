/**
 * test/register
 */
"use strict";

const storage = require("@dictadata/storage-junctions");
const { logger } = require("@dictadata/storage-junctions").utils;

const RedshiftJunction = require("../storage/junctions/redshift/redshift-junction");

logger.info("--- adding RedshiftJunction to storage cortex");
storage.use("redshift", RedshiftJunction);
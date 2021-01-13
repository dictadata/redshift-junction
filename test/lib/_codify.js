/**
 * test/codify
 */
"use strict";

const storage = require('@dictadata/storage-junctions');
const logger = require('../logger');

const fs = require('fs/promises');
const stream = require('stream/promises');


module.exports = async function (options) {

  logger.info(">>> create junction");
  var j1 = await storage.activate(options.source.smt, options.source.options);

  try {
    // *** the normal way is to ask the junction to do it
    logger.info(">>> getEncoding");
    let encoding1 = await j1.getEncoding();

    logger.debug(JSON.stringify(encoding1, null, "  "));
    logger.info(">>> save encoding to " + options.outputFile1);
    await fs.writeFile(options.outputFile1, JSON.stringify(encoding1, null, "  "), "utf8");

    // *** stream some data to the codifier
    logger.info(">>> create streams");
    var reader = j1.createReadStream({ max_read: (options.source.options && options.source.options.max_read) || 100 });
    var codify = j1.getCodifyWriter(options.codify || null);

    logger.info(">>> start pipe");
    await stream.pipeline(reader, codify);

    let encoding2 = await codify.getEncoding();

    logger.debug(JSON.stringify(encoding2, null, "  "));
    logger.info(">>> save encoding to " + options.outputFile2);
    await fs.writeFile(options.outputFile2, JSON.stringify(encoding2, null, "  "), "utf8");

    logger.info(">>> completed");
  }
  catch (err) {
    logger.error('!!! request failed: ' + err.message);
  }
  finally {
    await j1.relax();
  }

};

/**
 * redshift/junction
 */
"use strict";

const {
  StorageJunction, StorageResults, StorageError, Engram, logger
} = require('@dictadata/storage-junctions');

const RedshiftReader = require("./reader");
const RedshiftWriter = require("./writer");
const encoder = require("./encoder");
const sqlQuery = require("./sql_query");

const odbc = require('odbc');


module.exports = class RedshiftJunction extends StorageJunction {

  /**
   *
   * @param {*} SMT 'redshift|host=address;user=name;password=xyz;database=dbname;...|table|key' or an Engram object
   * @param {*} options
   */
  constructor(SMT, options = null) {
    super(SMT, options);
    logger.debug("RedshiftJunction");

    this._readerClass = RedshiftReader;
    this._writerClass = RedshiftWriter;

    this.poolConfig = {
      connectionString: this.engram.smt.locus,
      connectionTimeout: this.options.connectionTimeout || 10,
      loginTimeout: this.options.logintTimeout || 10,
      initialSize: 4,
      incrementSize: 2
    };

    this.pool = null;
  }

  async connect() {
    if (!this.pool)
      this.pool = await odbc.pool(this.poolConfig);
    return this.pool.connect();
  }

  async query(sql) {
    if (!this.pool)
      this.pool = await odbc.pool(this.poolConfig);
    return this.pool.query(sql);
  }

  async relax() {
    logger.debug("RedshiftJunction.relax");

    // release an resources
    if (this.pool)
      await this.pool.close();
  }

  /**
   *  Get the encoding for the storage node.
   *  Possibly make a call to the source to acquire the encoding definitions.
   */
  async getEncoding() {
    logger.debug("RedshiftJunction.getEncoding");

    try {
      // fetch encoding form storage source
      let conn = await this.connect();

      const columns = await conn.columns(null, null, this.engram.smt.schema, null);
      logger.debug(JSON.stringify(columns, replacer, "  "));

      for (let i = 0; i < columns.length; i++) {
        let field = encoder.storageField(columns[i]);
        this.engram.add(field);
      }

      conn.close();
      return this.engram;
    }
    catch (err) {
      if (err.errno === 1146)  // ER_NO_SUCH_TABLE
        return 'not found';

      logger.error(err.odbcErrors ? err.odbcErrors[0].message : err.message);
      throw err;
    }
  }

  /**
   * Sets the encoding for the storage node.
   * Possibly sending the encoding definitions to the source.
   * @param {*} encoding
   */
  async putEncoding(encoding) {
    logger.debug("RedshiftJunction.putEncoding");

    try {
      // check if table already exists
      let conn = await this.connect();

      const tables = await conn.tables(null, null, this.engram.smt.schema, null);
      logger.debug(JSON.stringify(tables, replacer, "  "));

      if (tables.length > 0) {
        return 'schema exists';
      }

      let engram = new Engram(this.engram);
      engram.replace(encoding);

      // create table
      let sql = sqlQuery.sqlCreateTable(engram);
      let results = await conn.query(sql);

      conn.close();
      this.engram.replace(encoding);
      return this.engram;
    }
    catch(err) {
      logger.error(err.odbcErrors ? err.odbcErrors[0].message : err.message);
      throw err;
    }
  }

  /**
   *
   * @param {*} construct
   */
  async store(construct, options = null) {
    logger.debug("RedshiftJunction.store");

    if (this.engram.keyof === 'uid' || this.engram.keyof === 'key')
      throw new StorageError({ statusCode: 400 }, "unique keys not supported");
    if (typeof construct !== "object")
      throw new StorageError({statusCode: 400}, "Invalid parameter: construct is not an object");

    try {
      if (Object.keys(this.engram.fields).length == 0)
        await this.getEncoding();

      let sql = sqlQuery.sqlInsert(this.engram, construct);
      let results = await this.query(sql);

      // check if row was inserted
      return new StorageResults( (results.count > 0) ? "ok" : "not stored", null, null, results);
    }
    catch(err) {
      if (err.errno === 1062) {  // ER_DUP_ENTRY
        return new StorageResults('duplicate', null, null, err);
      }

      logger.error(err.odbcErrors ? err.odbcErrors[0].message : err.message);
      throw err;
    }
  }

  /**
   *
   */
  async recall(options = null) {
    logger.debug("RedshiftJunction.recall");

    if (this.engram.keyof === 'uid' || this.engram.keyof === 'key')
      throw new StorageError({ statusCode: 400 }, "unique keys not supported");

    try {
      if (Object.keys(this.engram.fields).length == 0)
        await this.getEncoding();

      let sql = "SELECT * FROM " + this.engram.smt.schema + sqlQuery.sqlWhereFromKey(this.engram, options);
      let rows = await this.query(sql);

      return new StorageResults( (rows.length > 0) ? "ok" : "not found", rows[0]);
    }
    catch(err) {
      logger.error(err.odbcErrors ? err.odbcErrors[0].message : err.message);
      throw err;
    }
  }

  /**
   *
   * @param {*} options options.pattern
   */
  async retrieve(options = null) {
    logger.debug("RedshiftJunction.retrieve");
    let pattern = options && (options.pattern || options || {});

    try {
      if (Object.keys(this.engram.fields).length == 0)
        await this.getEncoding();

      let sql = sqlQuery.sqlSelectWithPattern(this.engram, pattern);
      let rows = await this.query(sql);

      return new StorageResults((rows.length > 0) ? "retreived" : "not found", rows);
    }
    catch(err) {
      logger.error(err.odbcErrors ? err.odbcErrors[0].message : err.message);
      throw err;
    }
  }

  /**
   *
   */
  async dull(options = null) {
    logger.debug("RedshiftJunction.dull");
    if (!options) options = {};

    if (this.engram.keyof === 'uid' || this.engram.keyof === 'key')
      throw new StorageError({ statusCode: 400 }, "unique keys not supported");

    try {
      if (Object.keys(this.engram.fields).length == 0)
        await this.getEncoding();

      let results = null;
      if (this.engram.keyof === 'list' || this.engram.keyof === 'all') {
        // delete construct by ID
        let sql = "DELETE FROM " + this.engram.smt.schema + sqlQuery.sqlWhereFromKey(this.engram, options);
        results = await this.query(sql);
      }
      else {
        // delete all constructs in the .schema
        let sql = "TRUNCATE " + this.engram.smt.schema + ";";
        results = await this.query(sql);
      }

      return new StorageResults((results.count > 0) ? "ok" : "not found", null, null, results);
    }
    catch (err) {
      logger.error(err.odbcErrors ? err.odbcErrors[0].message : err.message);
      throw err;
    }
  }

};

function replacer(key, value) {
  // Filtering out properties
  // eslint-disable-next-line valid-typeof
  if (typeof value === "bigint") {
    return Number(value);
  }

  return value;
}

const Store = require('tablestore')
const Client = require('./Client')
const Table = require('./Table')
const Model = require('./Model')

/**
 * TableStore 类
 */
class TableStore extends Client {
  constructor (options) {
    super(options)

    // 表集合
    this.tables = {}

    // 模型集合
    this.models = {}
  }

  /**
   * 同步 tables 数据
   */
  async sync () {
    let result = await this.listTable();
    let tableNames = result.table_names || [];
    tableNames.forEach((name) => this.defineTable(name));
  }

  /**
   * 定义模型
   */
  defineModel (modelName, props, tableOptions) {
    this.models[modelName] = Model.define(modelName, props, tableOptions, this)
  }

  /**
   * 定义数据表
   */
  defineTable (tableName, tableOptions) {
    this.tables[tableName] = new Table(tableName, tableOptions).setStore(this)
  }
}

module.exports = TableStore

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
   * 同步 meta 数据
   */
  sync () {

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

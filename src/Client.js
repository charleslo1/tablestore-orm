const Store = require('tablestore')

/**
 * Client 类
 */
class Client {
  constructor (options) {
    // 连接
    this.connect(options)
  }

  /**
   * 初始化
   * @param  {Object} options 链接参数
   */
  __init (options) {
    if (options === this) return
    // options
    Object.assign(this, {
      accessKeyId: '',
      accessKeySecret: '',
      endpoint: '',
      instancename: '',
      maxRetries: 20
    }, options)
  }

  /**
   * 初始化连接
   * @param  {Object} options 连接参数
   */
  connect (options = this) {
    // init
    this.__init(options)

    // client connection
    this.conn = new Store.Client({
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.accessKeySecret,
      endpoint: this.endpoint,
      instancename: this.instancename
    })

    return this
  }

  /**
   * 执行请求命令
   * @param  {String} command 请求命令
   * @param  {Object} params  请求参数
   */
  request (command, params, fn) {
    if (typeof(this.conn[command]) !== 'function') throw new Error('command 参数不正确！')

    return new Promise((resolve, reject) => {
      this.conn[command](params, function (err, data) {
        if (err) reject(err)
        resolve(data)
      })
    })
  }

  /**
   * 获取表名列表
   */
  listTable () {
    return this.request('listTable', {})
  }

  /**
   * 查询表描述信息（DescribeTable）
   * @param  {Object} params 参数（参考官网API）
   */
  describeTable (params) {
    return this.request('describeTable', params)
  }

  /**
   * 查询表描述信息（DescribeTable 的别名）
   * @param  {Object} params 参数（参考官网API）
   */
  getTable (params) {
    return this.describeTable(params)
  }

  /**
   * 创建数据表
   * @param  {Object} params 参数（参考官网API）
   */
  createTable (params) {
    return this.request('createTable', params)
  }

  /**
   * 更新数据表
   * @param  {Object} params 参数（参考官网API）
   */
  updateTable (params) {
    return this.request('updateTable', params)
  }

  /**
   * 删除数据表
   * @param  {Object} params 参数（参考官网API）
   */
  deleteTable (params) {
    return this.request('deleteTable', params)
  }

  /**
   * 插入一行数据
   * @param  {Object} params 参数（参考官网API）
   */
  putRow (params) {
    return this.request('putRow', params)
  }

  /**
   * 读取一行数据
   * @param  {Object} params 参数（参考官网API）
   */
  getRow (params) {
    return this.request('getRow', params)
  }

  /**
   * 更新一行数据
   * @param  {Object} params 参数（参考官网API）
   */
  updateRow (params) {
    return this.request('updateRow', params)
  }

  /**
   * 删除一行数据
   * @param  {Object} params 参数（参考官网API）
   */
  deleteRow (params) {
    return this.request('deleteRow', params)
  }

  /**
   * 批量读
   * @param  {Object} params 参数（参考官网API）
   */
  batchGetRow (params) {
    return this.request('batchGetRow', params)
  }

  /**
   * 批量写
   * @param  {Object} params 参数（参考官网API）
   */
  batchWriteRow (params) {
    return this.request('batchWriteRow', params)
  }

  /**
   * 范围读
   * @param  {Object} params 参数（参考官网API）
   */
  getRange (params) {
    return this.request('getRange', params)
  }

  /**
   * 数据类型
   * @type {Object}
   */
  static gettype () {
    return ''
  }
}

/**
 * 数据类型
 * @type {Object}
 */
Client.dataTypes = Client.prototype.dataTypes = {
  String: 'Stirng',
  Integer: Store.Long,
  Dobule: Number,
  Boolean: Boolean,
  Binary: Buffer
}

module.exports = Client

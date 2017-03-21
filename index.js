const parallel = require('parallel-multistream')
const dateRange = require('date-range-array')
const stream = require('readable-stream')
const gunzip = require('gunzip-maybe')
const S3Lister = require('s3-lister')
const pumpify = require('pumpify')
const assert = require('assert')
const split = require('split2')
const eol = require('os').EOL
const knox = require('knox')
const pump = require('pump')
const util = require('util')

module.exports = LogFetcher

// Fetch logs from S3
// obj -> rstream
function LogFetcher (opts) {
  if (!(this instanceof LogFetcher)) return new LogFetcher(opts)

  assert.equal(typeof opts, 'object', 'opts should be an object')
  assert.equal(typeof opts.from, 'string', 'opts.from should be a string')
  assert.equal(typeof opts.until, 'string', 'opts.until should be a string')
  assert.equal(typeof opts.key, 'string', 'opts.key should be a string')
  assert.equal(typeof opts.secret, 'string', 'opts.secret should be a string')
  assert.equal(typeof opts.bucket, 'string', 'opts.bucket should be a string')

  this._fetchComplete = false;
  this._lastKey = null;
  this._fetchingDateIndex = 0;
  this._forwarding = false
  this._destroyed = false
  this._drained = false
  this._dates = dateRange(opts.from, opts.until)
  this._client = knox.createClient({
    key: opts.key,
    secret: opts.secret,
    bucket: opts.bucket
  })
  this._prefix = opts.prefix

  stream.Readable.call(this, opts)
}
util.inherits(LogFetcher, stream.Readable)

// abort a stream before it ends
// err? -> null
LogFetcher.prototype.destroy = function (err) {
  if (this._destroyed) return
  this._destroyed = true
  var self = this
  process.nextTick(function () {
    if (err) self.emit('error', err)
    self.emit('close')
  })
}

// start the reader
// null -> null
LogFetcher.prototype._read = function () {
  this._drained = true
  this._forward()
}

// start forwarding data from S3
// null -> null
LogFetcher.prototype._forward = function () {
  if (this._forwarding || !this._drained) return
  this._forwarding = true

  const self = this
  const files = []
  let prefix = ''
  if (this._prefix) {
    prefix += this._prefix + '/'
  }
  prefix += this._dates[this._fetchingDateIndex]
  const listerOpts = { prefix: prefix, start: this._lastKey }

  const rs = new S3Lister(this._client, listerOpts)
  var counter = 0

  rs.on('data', function (data) {
    if (!data.Size) return
    counter++
    self._client.getFile(data.Key, function (err, fileStream) {
      if (err) return self.emit('error', err)
      if (self.ended) return

      files.push(pumpify(fileStream, gunzip()))

      const ended = rs.ended
      if (ended && (files.length === counter)) {
        if (self._dates.length > 1) {
          if(counter < 1000) {
            self._fetchingDateIndex++;
          }
          if(self._fetchingDateIndex >= self._dates.length) {
            self._fetchComplete = true;
          }
          self._lastKey = data.Key;
        }
        else if (counter === 1000) {
          self._lastKey = data.Key;
        }
        else {
          self._lastKey = null;
          self._fetchComplete = true;
        }
        streamFiles()
      }
    })
  })

  function streamFiles () {
    const ts = split()
    const rs = parallel(files)
    pump(rs, ts)
    ts.on('data', function (line) {
      self.push(line + eol)
    })
    ts.on('end', function () {
      if (self._fetchComplete) {
        self.push(null);
      }
      else {
        self._forward();
      }
    })
    ts.on('error', function (err) {
      self.emit('error', err)
    })
  }
}

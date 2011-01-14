var fs = require('fs')
  , mmap = require('mmap')
  , consts = require('./consts')

/**
 * NOTE: this constructor does synchronous reads to the FS since it is assumed
 * it will be called at program startup
 */
var GeoIP = exports.GeoIP = function(filename) {
  this.fd = fs.openSync(filename, 'r')
  var size = fs.fstatSync(this.fd).size
  this.buffer = new mmap.Buffer(size, mmap.PROT_READ, mmap.MAP_SHARED, this.fd, 0)
  this._seek(0)
  this._setupSegments()
}

GeoIP.prototype._seek = function(pos) {
  this._off = pos
}

GeoIP.prototype._seekRelative = function(pos) {
  this._off = this._off + pos
}

GeoIP.prototype._read = function(count) {
  if (count === undefined) {
    return this.buffer[this._off++]
  } else {
    var b = new Buffer(count)
    for (var i = 0; i < count; ++i) {
      b[i] = this.buffer[this._off + i]
    }
    this._seekRelative(count)
    return b
  }
}

/**
 * Parses the database file to determine what kind of database is being used and setup
 * segment sizes and start points that will be used by the seek*() methods later.
 */
GeoIP.prototype._setupSegments = function() {
  this._databaseType = consts.COUNTRY
  this._recordLength = consts.STANDARD_RECORD_LENGTH

  this._seek(this.buffer.length - 3)
  for (var i = 0; !this._databaseType && i < consts.STRUCTURE_INFO_MAX_SIZE; ++i) {
    var delim = this._read(3)
    if (delim.toString('binary') === '\u00ff\u00ff\u00ff') {
      this._databaseType = this._read()

      if (this._databaseType >= 106) {
        // backwards compatibility with databases from April 2003 and earlier
        this._databaseType -= 105
      }

      if (this._databaseType === consts.REGION_EDITION_REV0) {
        this._databaseSegments = consts.STATE_BEGIN_REV0
      }

      if (this._databaseType === consts.REGION_EDITION_REV1) {
        this._databaseSegments = consts.STATE_BEGIN_REV1
      }

      switch (this._databaseType) {
        case consts.CITY_EDITION_REV0:
        case consts.CITY_EDITION_REV1:
        case consts.ORG_EDITION:
        case consts.ISP_EDITION:
        case consts.ASNUM_EDITION:
          this._databaseSegments = 0
          var buf = this._read(consts.SEGMENT_RECORD_LENGTH)
          for (var j = 0; j < consts.SEGMENT_RECORD_LENGTH; ++j) {
            this._databaseSegments += buf[j] << (j * 8)
          }
          if (this._databaseType === consts.ORG_EDITION
              || this._databaseType === consts.ISP_EDITION) {
            this._recordLength = consts.ORG_RECORD_LENGTH
          }
        default:
      }
    } else {
      this._seekRelative(-4)
    }
  }
}

/**
 * Using the record length and appropriate start points, seek to the
 * country that corresponds to the converted IP address integer.
 */
GeoIP.prototype._seekCountry = function(ipnum) {
  var offset = 0
    , buf
    , x

  for (var depth = 31; depth >= 0; --depth) {
    this._seek(2 * this._recordLength * offset)
    buf = this._read(2 * this._recordLength)

    x = [0, 0]

    for (var i = 0; i < 2; ++i) {
      for (var j = 0; j < this._recordLength; ++j) {
        x[i] += buf[this._recordLength * i + j] << (j * 8)
      }
    }

    if (ipnum & (1 << depth)) {
      if (x[1] >= this._databaseSegments) {
        return x[1]
      }
      offset = x[1]
    } else {
      if (x[0] >= this._databaseSegments) {
        return x[0]
      }
      offset = x[0]
    }
  }
}

/**
 * populate location dict for converted IP.
 */
GeoIP.prototype._getRecord = function(ipnum) {
  var seekCountry = this._seekCountry(ipnum)

  if (seekCountry === this._databaseSegments) {
    return
  }

  this._seek(seekCountry + (2 * this._recordLength - 1) * this._databaseSegments)

  var buf = this._read(consts.FULL_RECORD_LENGTH)
  var record = {}

  var bufPos = 0

  var char = buf[bufPos]
  record.country_code = consts.COUNTRY_CODES[char]
  record.country_code3 = consts.COUNTRY_CODES3[char]
  record.country_name = consts.COUNTRY_NAMES[char]
  bufPos += 1

  function readString() {
    var len = 0
    while (buf[bufPos+len] !== 0) {
      len++
    }
    if (len == 0) {
    }
    var str = buf.toString('utf8', bufPos, bufPos + len)

    bufPos += len + 1
    return (str.length > 0 ? str : null)
  }

  function readNumber() {
    var n = 0
    for (var j = 0; j < 3; ++j) {
      n += (buf[bufPos] << (j * 8))
      bufPos += 1
    }
    return n
  }

  record.region_name = readString() 
  record.city_name = readString() 
  record.postal_code = readString()

  record.latitude = (readNumber()/10000.0) - 180
  record.longitude = (readNumber()/10000.0) - 180

  if (this._databaseType === consts.CITY_EDITION_REV1) {
    if (record.country_code == 'US') {
      var combo = readNumber()
      record.dma_code = Math.floor(combo/1000)
      record.area_code = combo % 1000
    }
  } else {
    record.dma_code = 0
    record.area_code = 0
  }

  return record
}

/**
 * Look up the record for a given IP address.
 * Use this method if you have a City database.
 */
GeoIP.prototype.getRecordByAddr = function(ip) {
  var ipnum = ip2num(ip)
  return this._getRecord(ipnum)
}

/**
 * This prevent the repl from printing the contents of the mmap'd buffer
 */
GeoIP.prototype.inspect = function() {
  return "<GeoIP>"
}

/**
 * Convert a IPv4 address into a 32-bit integer.
 */
function ip2num(ip) {
  var parts = ip.split('.')
  return parseInt(parts[0]) * 16777216
      + parseInt(parts[1]) * 65536
      + parseInt(parts[2]) * 256
      + parseInt(parts[3])
}

// For debugging
exports.ip2num = ip2num

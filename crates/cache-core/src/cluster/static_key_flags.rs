//! Generated Redis 8.2.11 static per-key access flags.
//! Dynamic flags for SET/BITFIELD are handled in key_flags.rs.
pub(super) fn flags(name: &str, ordinal: usize) -> &'static [&'static str] {
    match name {
        "append" => &["RW", "insert"],
        "bitcount" => &["RO", "access"],
        "bitfield_ro" => &["RO", "access"],
        "bitop" => {
            if ordinal == 0 {
                &["OW", "update"]
            } else {
                &["RO", "access"]
            }
        }
        "bitpos" => &["RO", "access"],
        "blmove" => {
            if ordinal == 0 {
                &["RW", "access", "delete"]
            } else {
                &["RW", "insert"]
            }
        }
        "blmpop" => &["RW", "access", "delete"],
        "blpop" => &["RW", "access", "delete"],
        "brpop" => &["RW", "access", "delete"],
        "brpoplpush" => {
            if ordinal == 0 {
                &["RW", "access", "delete"]
            } else {
                &["RW", "insert"]
            }
        }
        "bzmpop" => &["RW", "access", "delete"],
        "bzpopmax" => &["RW", "access", "delete"],
        "bzpopmin" => &["RW", "access", "delete"],
        "copy" => {
            if ordinal == 0 {
                &["RO", "access"]
            } else {
                &["OW", "update"]
            }
        }
        "decr" => &["RW", "access", "update"],
        "decrby" => &["RW", "access", "update"],
        "del" => &["RM", "delete"],
        "dump" => &["RO", "access"],
        "exists" => &["RO"],
        "expire" => &["RW", "update"],
        "expireat" => &["RW", "update"],
        "expiretime" => &["RO", "access"],
        "geoadd" => &["RW", "update"],
        "geodist" => &["RO", "access"],
        "geohash" => &["RO", "access"],
        "geopos" => &["RO", "access"],
        "georadius" => {
            if ordinal == 0 {
                &["RO", "access"]
            } else {
                &["OW", "update"]
            }
        }
        "georadius_ro" => &["RO", "access"],
        "georadiusbymember" => {
            if ordinal == 0 {
                &["RO", "access"]
            } else {
                &["OW", "update"]
            }
        }
        "georadiusbymember_ro" => &["RO", "access"],
        "geosearch" => &["RO", "access"],
        "geosearchstore" => {
            if ordinal == 0 {
                &["OW", "update"]
            } else {
                &["RO", "access"]
            }
        }
        "get" => &["RO", "access"],
        "getbit" => &["RO", "access"],
        "getdel" => &["RW", "access", "delete"],
        "getex" => &["RW", "access", "update"],
        "getrange" => &["RO", "access"],
        "getset" => &["RW", "access", "update"],
        "hdel" => &["RW", "delete"],
        "hexists" => &["RO"],
        "hexpire" => &["RW", "update"],
        "hexpireat" => &["RW", "update"],
        "hexpiretime" => &["RO", "access"],
        "hget" => &["RO", "access"],
        "hgetall" => &["RO", "access"],
        "hgetdel" => &["RW", "access", "delete"],
        "hgetex" => &["RW", "access", "update"],
        "hincrby" => &["RW", "access", "update"],
        "hincrbyfloat" => &["RW", "access", "update"],
        "hkeys" => &["RO", "access"],
        "hlen" => &["RO"],
        "hmget" => &["RO", "access"],
        "hmset" => &["RW", "update"],
        "hpersist" => &["RW", "update"],
        "hpexpire" => &["RW", "update"],
        "hpexpireat" => &["RW", "update"],
        "hpexpiretime" => &["RO", "access"],
        "hpttl" => &["RO", "access"],
        "hrandfield" => &["RO", "access"],
        "hscan" => &["RO", "access"],
        "hset" => &["RW", "update"],
        "hsetex" => &["RW", "update"],
        "hsetnx" => &["RW", "insert"],
        "hstrlen" => &["RO"],
        "httl" => &["RO", "access"],
        "hvals" => &["RO", "access"],
        "incr" => &["RW", "access", "update"],
        "incrby" => &["RW", "access", "update"],
        "incrbyfloat" => &["RW", "access", "update"],
        "lcs" => &["RO", "access"],
        "lindex" => &["RO", "access"],
        "linsert" => &["RW", "insert"],
        "llen" => &["RO"],
        "lmove" => {
            if ordinal == 0 {
                &["RW", "access", "delete"]
            } else {
                &["RW", "insert"]
            }
        }
        "lmpop" => &["RW", "access", "delete"],
        "lpop" => &["RW", "access", "delete"],
        "lpos" => &["RO", "access"],
        "lpush" => &["RW", "insert"],
        "lpushx" => &["RW", "insert"],
        "lrange" => &["RO", "access"],
        "lrem" => &["RW", "delete"],
        "lset" => &["RW", "update"],
        "ltrim" => &["RW", "delete"],
        "mget" => &["RO", "access"],
        "mset" => &["OW", "update"],
        "msetnx" => &["OW", "insert"],
        "persist" => &["RW", "update"],
        "pexpire" => &["RW", "update"],
        "pexpireat" => &["RW", "update"],
        "pexpiretime" => &["RO", "access"],
        "pfadd" => &["RW", "insert"],
        "pfcount" => &["RW", "access"],
        "pfdebug" => &["RW", "access"],
        "pfmerge" => {
            if ordinal == 0 {
                &["RW", "access", "insert"]
            } else {
                &["RO", "access"]
            }
        }
        "psetex" => &["OW", "update"],
        "pttl" => &["RO", "access"],
        "rename" => {
            if ordinal == 0 {
                &["RW", "access", "delete"]
            } else {
                &["OW", "update"]
            }
        }
        "renamenx" => {
            if ordinal == 0 {
                &["RW", "access", "delete"]
            } else {
                &["OW", "insert"]
            }
        }
        "restore" => &["OW", "update"],
        "rpop" => &["RW", "access", "delete"],
        "rpoplpush" => {
            if ordinal == 0 {
                &["RW", "access", "delete"]
            } else {
                &["RW", "insert"]
            }
        }
        "rpush" => &["RW", "insert"],
        "rpushx" => &["RW", "insert"],
        "sadd" => &["RW", "insert"],
        "scard" => &["RO"],
        "sdiff" => &["RO", "access"],
        "sdiffstore" => {
            if ordinal == 0 {
                &["OW", "update"]
            } else {
                &["RO", "access"]
            }
        }
        "setbit" => &["RW", "access", "update"],
        "setex" => &["OW", "update"],
        "setnx" => &["OW", "insert"],
        "setrange" => &["RW", "update"],
        "sinter" => &["RO", "access"],
        "sintercard" => &["RO", "access"],
        "sinterstore" => {
            if ordinal == 0 {
                &["OW", "update"]
            } else {
                &["RO", "access"]
            }
        }
        "sismember" => &["RO"],
        "smembers" => &["RO", "access"],
        "smismember" => &["RO", "access"],
        "smove" => {
            if ordinal == 0 {
                &["RW", "access", "delete"]
            } else {
                &["RW", "insert"]
            }
        }
        "sort" => {
            if ordinal == 0 {
                &["RO", "access"]
            } else {
                &["OW", "update"]
            }
        }
        "sort_ro" => &["RO", "access"],
        "spop" => &["RW", "access", "delete"],
        "srandmember" => &["RO", "access"],
        "srem" => &["RW", "delete"],
        "sscan" => &["RO", "access"],
        "strlen" => &["RO"],
        "substr" => &["RO", "access"],
        "sunion" => &["RO", "access"],
        "sunionstore" => {
            if ordinal == 0 {
                &["OW", "update"]
            } else {
                &["RO", "access"]
            }
        }
        "touch" => &["RO"],
        "ttl" => &["RO", "access"],
        "type" => &["RO"],
        "unlink" => &["RM", "delete"],
        "watch" => &["RO"],
        "zadd" => &["RW", "update"],
        "zcard" => &["RO"],
        "zcount" => &["RO", "access"],
        "zdiff" => &["RO", "access"],
        "zdiffstore" => {
            if ordinal == 0 {
                &["OW", "update"]
            } else {
                &["RO", "access"]
            }
        }
        "zincrby" => &["RW", "access", "update"],
        "zinter" => &["RO", "access"],
        "zintercard" => &["RO", "access"],
        "zinterstore" => {
            if ordinal == 0 {
                &["OW", "update"]
            } else {
                &["RO", "access"]
            }
        }
        "zlexcount" => &["RO", "access"],
        "zmpop" => &["RW", "access", "delete"],
        "zmscore" => &["RO", "access"],
        "zpopmax" => &["RW", "access", "delete"],
        "zpopmin" => &["RW", "access", "delete"],
        "zrandmember" => &["RO", "access"],
        "zrange" => &["RO", "access"],
        "zrangebylex" => &["RO", "access"],
        "zrangebyscore" => &["RO", "access"],
        "zrangestore" => {
            if ordinal == 0 {
                &["OW", "update"]
            } else {
                &["RO", "access"]
            }
        }
        "zrank" => &["RO", "access"],
        "zrem" => &["RW", "delete"],
        "zremrangebylex" => &["RW", "delete"],
        "zremrangebyrank" => &["RW", "delete"],
        "zremrangebyscore" => &["RW", "delete"],
        "zrevrange" => &["RO", "access"],
        "zrevrangebylex" => &["RO", "access"],
        "zrevrangebyscore" => &["RO", "access"],
        "zrevrank" => &["RO", "access"],
        "zscan" => &["RO", "access"],
        "zscore" => &["RO", "access"],
        "zunion" => &["RO", "access"],
        "zunionstore" => {
            if ordinal == 0 {
                &["OW", "update"]
            } else {
                &["RO", "access"]
            }
        }
        _ => &[],
    }
}

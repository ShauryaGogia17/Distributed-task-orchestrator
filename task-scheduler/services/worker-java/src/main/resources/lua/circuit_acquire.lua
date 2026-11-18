
if redis.call('EXISTS', KEYS[1]) == 0 then return {1, '', '', 0} end
local now = redis.call('TIME')
local ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local until_ms = tonumber(redis.call('HGET', KEYS[1], 'until') or '0')
local probe_until = tonumber(redis.call('HGET', KEYS[1], 'probe_until') or '0')
if ms < until_ms then return {0, '', '', until_ms - ms} end
if ms < probe_until then return {0, '', '', probe_until - ms} end
local epoch = redis.call('HGET', KEYS[1], 'epoch')
redis.call('HSET', KEYS[1], 'state', 'HALF_OPEN', 'probe', ARGV[1],
 'probe_until', ms + tonumber(ARGV[2]))
return {1, epoch, ARGV[1], 0}

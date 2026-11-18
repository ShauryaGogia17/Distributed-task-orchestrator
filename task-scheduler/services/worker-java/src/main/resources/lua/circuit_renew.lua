
local now = redis.call('TIME')
local ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[1]
 or redis.call('HGET', KEYS[1], 'probe') ~= ARGV[2]
 or tonumber(redis.call('HGET', KEYS[1], 'probe_until') or '0') <= ms then return 0 end
redis.call('HSET', KEYS[1], 'probe_until', ms + tonumber(ARGV[3]))
return 1

--1.1 voucher id
local voucherId = ARGV[1]

--1.2 user id
local userId = ARGV[2]

--1.3 order id
local orderId = ARGV[3]

--2.1 stock key
local stockKey = 'seckill:stock:' .. voucherId
--2.2 order key
local orderKey = 'seckill:order:' .. voucherId

local stockStr = redis.call('get',stockKey)
if stockStr == false or stockStr == nil then
    return 1
end
-- 3.1 determine whether the stock is enough
local stock = tonumber(stockStr)
if stock == nil or stock <= 0 then
    return 1
end
-- 3.2 determine whether the user has purchased (SISMENBER)
if redis.call('sismember',orderKey,userId)==1 then
    return 2
end

-- 4.1 decrease stock
redis.call('incrby',stockKey,-1)

-- 4.2 add order
redis.call('sadd',orderKey,userId)


--4.3 send message to the queue, XADD stream.orders * k1 v1 k2 v2 ....
redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)

return 0
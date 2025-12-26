package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;
    @Override
    public Result queryById(Long id) {
        // use mutex lock to solve the cache penetration
//        Shop shop = cacheClient
//                .queryWithPassThrough(CACHE_SHOP_KEY,id ,Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        use logical expiration to solve cache breakdown
        Shop shop = cacheClient
                .queryWithLogicalExpire(CACHE_SHOP_KEY,id ,Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null){
            return Result.fail("shop not exist");
        }
        return Result.ok(shop);
    }
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
//    private Shop queryWithLogicalExpire(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        // 1. query shop cache from the redis
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2. determine whether the cache exists
//        if (StrUtil.isBlank(shopJson)) {
//            // 3. if not exist return null
//            return null;
//        }
//
//        // 4. if hit, parse the json
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        // 5. determine whether the cache is expired
//        if (expireTime.isAfter(LocalDateTime.now())) {
//            // 5.1 if not expire, return the shop
//            return shop;
//        }
//
//        // 6. if expired, reconstruct the expiration time
//        // 6.1 get the lock
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(lockKey);
//        // 6.2 if success, make a single thread
//        // 6.3 if not success, reutrn the shop
//        if (isLock) {
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                // rebuild the cache
//                try {
//                    this.saveShop2Redis(id, 20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    // release the lock
//                    unLock(lockKey);
//                }
//            });
//        }
//        // 8. return shop
//        return shop;
//    }
//    private Shop queryWithMutex(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        // 1. query shop cache from the redis
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2. determine whether the cache exists
//        if (StrUtil.isNotBlank(shopJson)) {
//            // 3. query shop from the database
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        // determine whether the cache is empty
//        if (shopJson != null) {
//            return null;
//        }
//
//        // 4. Implement cache reconstruction
//        // 4.1 get the mutex lock
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(lockKey);
//        // 4.2 determine if we get the lock successfully
//        Shop shop = null;
//        try {
//            if (!isLock) {
//                // 4.3 if not get the lock, sleep and try again
//                Thread.sleep(50);
//                return queryWithMutex(id);
//            }
//
//            shop = getById(id);
//            // 5. if not exist return error
//            if (shop == null) {
//                // 5.1 save empty cache to redis
//                stringRedisTemplate.opsForValue().set(key, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
//                return null;
//            }
//            // 6. if exists, save shop cache to redis
//            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        finally {
//            // 7. release the lock
//            unLock(lockKey);
//        }
//        // 8. return shop
//        return shop;
//    }
//    private boolean tryLock(String key) {
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1",10L, TimeUnit.SECONDS);
//        return BooleanUtil.isTrue(flag);
//    }
//    private void unLock(String key) {
//        stringRedisTemplate.delete(key);
//    }
//    public void saveShop2Redis(Long id,Long expireSeconds){
//        // 1. query shop from the database
//        Shop shop = getById(id);
//        // 2. encapsulate expiration time
//        RedisData redisData = new RedisData();
//        redisData.setData(shop);
//        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
//        // 3. save shop cache to redis
//        String key = CACHE_SHOP_KEY + id;
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
//    }
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id==null) {
            return Result.fail("shop id can not be null");
        }
        // 1. update database
        updateById(shop);
        // 2. delete cache
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // determine whether query by
        if(x==null || y==null){
            Page<Shop> page = query()
                    .eq("type_id",typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }

        // calculate page parameter
        int from = (current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current*SystemConstants.DEFAULT_PAGE_SIZE;
        log.info("GEO paging params => current={}, from={}, end={}", current, from, end);
        // query from the redis by distance, page
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // parse id
        if(results==null) return Result.ok(Collections.emptyList());
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list == null || list.isEmpty() || from >= list.size()) {
            return Result.ok(Collections.emptyList());
        }
        //extra the part in results from start to end
        List<Long> ids = new ArrayList<>(list.size());
        Map<String,Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        // query the shop by id
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }
}

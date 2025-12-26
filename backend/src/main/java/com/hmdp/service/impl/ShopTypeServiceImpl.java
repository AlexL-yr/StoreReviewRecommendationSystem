package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryTypeList() {
        String key = CACHE_SHOP_TYPE_KEY + "list";
        String listJson = stringRedisTemplate.opsForValue().get(key);

        if(StrUtil.isNotBlank(listJson)){
            List<ShopType> typeList= JSONUtil.toList(listJson, ShopType.class);
            return Result.ok(typeList);
        }
        List<ShopType> typeList = this.query()
                .orderByAsc("sort")
                .list();
        if(typeList==null||typeList.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(typeList));
        return Result.ok(typeList);
    }
}

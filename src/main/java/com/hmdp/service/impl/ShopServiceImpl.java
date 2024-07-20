package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

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
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
//        Shop shop = queryWithPassThrough(id);

        // 互斥锁完成缓存击穿
        Shop shop = queryWithMutux(id);
        if(shop==null){
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }

    public Shop queryWithMutux(Long id){
        //先从Redis中查，这里的常量值是固定的前缀 + 店铺id
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //如果不为空（查询到了），则转为Shop类型直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 判断命中的是否是空值,isNotBlank("")为false,所以要再用一个if判断d
        if (shopJson != null){
            //返回一个错误信息
            return null;
        }

        // 1.实现缓存重建
        // 1.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 1.2 判断是否获取成功
            if(!isLock){
                // 1.3 失败，则休眠重试
                Thread.sleep(50);
                return queryWithMutux(id);
            }
            // 模拟重建缓存延时
            Thread.sleep(200);
            //否则去数据库中查
            shop = getById(id);
            //查不到返回一个错误信息或者返回空都可以，根据自己的需求来
            if (shop == null){
                // 将空值写入redis,注意TTL时间要尽可能短,防止过多垃圾数据存在
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //查到了则转为json字符串
            String jsonStr = JSONUtil.toJsonStr(shop);
            //并存入redis，设置TTL
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, jsonStr,CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 释放互斥锁
            unLock(lockKey);
        }
        return shop;
    }


    // 缓存穿透函数
    public Shop queryWithPassThrough(Long id){
        //先从Redis中查，这里的常量值是固定的前缀 + 店铺id
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //如果不为空（查询到了），则转为Shop类型直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 判断命中的是否是空值,isNotBlank("")为false,所以要再用一个if判断d
        if (shopJson != null){
            //返回一个错误信息
            return null;
        }
        //否则去数据库中查
        Shop shop = getById(id);
        //查不到返回一个错误信息或者返回空都可以，根据自己的需求来
        if (shop == null){
            // 将空值写入redis,注意TTL时间要尽可能短,防止过多垃圾数据存在
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //查到了则转为json字符串
        String jsonStr = JSONUtil.toJsonStr(shop);
        //并存入redis，设置TTL
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, jsonStr,CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }




    private boolean tryLock(String key){
        // setIfAbsent就是redis中的setnx命令,该命令的作用是只在数据不存在时set成功,此处用setnx命令实现互斥锁
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
//        如果直接返回flag，当flag为时null，自动拆箱会导致空指针异常。
//        使用BooleanUtil.isTrue(flag)
//        1. 可以安全地处理null值，避免因null值导致的异常。
//        2. 如果flag为会返回Boolean.TRUE，否则返回false。
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}

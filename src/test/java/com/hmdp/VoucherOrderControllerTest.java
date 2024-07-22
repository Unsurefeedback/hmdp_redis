package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import lombok.Cleanup;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import javax.annotation.Resource;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.hutool.core.lang.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;

/**
 * @author weihanqiang
 * @date 2024/7/22
 */@SpringBootTest
@AutoConfigureMockMvc
class VoucherOrderControllerTest {
     @Resource
     private IUserService userService;

     @Resource
     private StringRedisTemplate stringRedisTemplate;

    /**
     * 在 Redis 中保存 1000 个用户信息并将其 token 写入文件中，方便测试多人秒杀业务
     */
    @Test
    void testMultiLogin() throws IOException {
        List<User> userList = userService.lambdaQuery().last("limit 1000").list();
        for (User user : userList) {
            String token = UUID.randomUUID().toString(true);
            UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
            Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                    CopyOptions.create().ignoreNullValue()
                            .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
            String tokenKey = LOGIN_USER_KEY + token;
            stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
            stringRedisTemplate.expire(tokenKey, 30, TimeUnit.MINUTES);
        }
        Set<String> keys = stringRedisTemplate.keys(LOGIN_USER_KEY + "*");
        @Cleanup FileWriter fileWriter = new FileWriter(System.getProperty("user.dir") + "\\tokens.txt");
        @Cleanup BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        assert keys != null;
        for (String key : keys) {
            String token = key.substring(LOGIN_USER_KEY.length());
            String text = token + "\n";
            bufferedWriter.write(text);
        }
    }

}

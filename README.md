# HyperRedis

HyperRedis 是一个用于学习 Redis 内部设计的 C++20 项目，主要参考《Redis 设计与实现（第二版）》中的数据结构、对象模型、数据库和命令执行流程。

当前项目重点是 `HyperRedisCore`：一个不依赖网络层的静态核心库。它已经包含底层数据结构、Redis 对象模型、数据库管理、多 DB 上下文和命令执行器。完整 Redis-like 网络服务器还没有接入，后续会在此核心库之上继续实现 RESP 解析/序列化和连接管理。

本项目的目标是学习 Redis 的结构和行为，不是生产级 Redis 替代品。

## 当前使用方式

当前推荐通过测试和 `CommandExecutor` 使用项目能力：

- 直接构建 `HyperRedisCore` 静态库
- 通过 GoogleTest 回归验证数据结构、对象层、数据库层和命令执行层
- 在代码中创建 `RedisManager`、`RedisClientContext` 和 `CommandExecutor`，用参数数组调用命令执行器并获得 `RespValue`

目前没有独立服务器可执行目标，也没有 TCP 监听、RESP 请求解析或客户端连接管理。

## 构建和测试

依赖：

- C++20 编译器
- CMake 3.20+
- 系统可发现的 `spdlog`
- 系统可发现的 `GTest`

常用命令：

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

如果使用 CLion 默认目录：

```bash
cmake --build cmake-build-debug
ctest --test-dir cmake-build-debug --output-on-failure
```

快速干净配置检查：

```bash
cmake -S . -B /tmp/hyperredis-check
cmake --build /tmp/hyperredis-check
ctest --test-dir /tmp/hyperredis-check --output-on-failure
```

## 项目结构

```text
include/hyper/
  datastructures/   底层数据结构
  storage/          RedisObject、RedisDb、RedisManager
  server/           RespValue、客户端上下文、命令执行器

src/
  storage/          存储与对象实现
  server/           命令执行核心实现

test/               GoogleTest 测试
```

`CMakeLists.txt` 显式维护源文件和测试目标列表。

## 已实现内容

### 底层数据结构

- `dict`：哈希表、渐进式 rehash、透明字符串查找、随机 key 获取
- `skipList`：按 score/rank 查询、遍历和区间删除
- `intset`：紧凑整数集合和整数编码升级
- `ziplist`：紧凑线性结构，支持插入、删除和区间删除
- `linked_list`：双向链表，用作 List 展开编码

### 存储和对象模型

- `RedisObject` 支持 String、List、Hash、Set、ZSet
- String 支持整数编码、追加、范围读写、整数/浮点自增和溢出检查
- List 支持 ziplist/linked-list 编码切换和常见列表操作
- Hash 支持 ziplist/dict 编码切换
- Set 支持 intset/dict 编码切换
- ZSet 支持 ziplist/skiplist 编码切换、score 查询、rank、range 和区间删除
- `RedisDb` 支持 key-value 主字典、过期字典、惰性过期、主动过期、TTL/PTTL、rename、random key 和清库
- `RedisManager` 支持多 DB 管理
- `RedisClientContext` 保存客户端当前 DB 选择

### 命令执行器

`CommandExecutor` 已支持命令名大小写归一、参数数量校验、Redis 风格错误返回和 `RespValue` 响应模型。

当前已实现的命令包括：

- 连接/DB/通用：`PING`、`SELECT`、`DBSIZE`、`DEL`、`EXISTS`、`TYPE`、`TTL`、`PTTL`、`PERSIST`、`EXPIRE`、`PEXPIRE`、`FLUSHDB`、`FLUSHALL`、`RANDOMKEY`、`RENAME`、`RENAMENX`
- String：`SET`、`GET`、`MGET`、`MSET`、`STRLEN`、`APPEND`、`INCR`、`DECR`、`INCRBY`、`INCRBYFLOAT`、`GETRANGE`、`SETRANGE`
- List：`LPUSH`、`RPUSH`、`LPOP`、`RPOP`、`LLEN`、`LRANGE`、`LINDEX`、`LSET`、`LINSERT`、`LREM`、`LTRIM`
- Hash：`HSET`、`HGET`、`HDEL`、`HLEN`、`HGETALL`、`HEXISTS`、`HKEYS`、`HVALS`
- Set：`SADD`、`SREM`、`SISMEMBER`、`SCARD`、`SMEMBERS`、`SPOP`、`SRANDMEMBER`
- ZSet：`ZADD`、`ZREM`、`ZSCORE`、`ZCARD`、`ZRANGE`

`EXPIRE`/`PEXPIRE` 已支持 `NX`、`XX`、`GT`、`LT` 条件选项。

## 当前进展

当前核心库和命令执行器已经覆盖 Redis 常用基础行为，并有测试回归：

- 底层数据结构测试
- RedisObject 编码转换和行为测试
- Redis 可见行为测试
- RedisDb、RedisManager、RedisClientContext 测试
- CommandExecutor 命令级测试

今天补齐并测试的命令：

- List：`LINDEX`、`LSET`、`LINSERT`、`LREM`、`LTRIM`
- Hash：`HEXISTS`、`HKEYS`、`HVALS`
- Set：`SPOP`、`SRANDMEMBER`

当前剩余的命令执行器 TODO 主要集中在 ZSet 高级命令：

- `ZRANK`
- `ZREVRANK`
- `ZCOUNT`
- `ZREVRANGE`
- `ZINCRBY`
- `ZREMRANGEBYRANK`
- `ZREMRANGEBYSCORE`

## 后续计划

近期重点：

- 补齐剩余 ZSet 命令执行器实现和测试
- 清理命令执行器中重复的容器访问模式
- 补齐 RESP 请求解析和响应序列化
- 增加完整服务器目标和网络连接管理

中长期方向：

- 接入 Muduo 或其他事件驱动网络层
- 探索基础持久化能力
- 继续完善 Redis 行为兼容测试

# go-ddcron
[![Goproxy.cn](https://goproxy.cn/stats/github.com/melf-xyzh/go-ddcron/badges/download-count.svg)](https://goproxy.cn)

### 简介

ddcron 是一个 Go语言分布式动态定时任务库

### 安装

```
go get -u github.com/melf-xyzh/go-ddcron/v1
```

### 使用

初始化CronClien

```go
// 初始化Redis
rdb := redis.NewClient(&redis.Options{
    Addr:     "127.0.0.1:6379",
    Password: "",
    DB:       0,
})

// 初始化数据库链接
dsn := "root:123456789@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"

config := &gorm.Config{}
config.Logger = logger.Default.LogMode(logger.Info)

db, err := gorm.Open(mysql.Open(dsn), config)
if err != nil {
    panic(err)
}

// 设置需要执行的函数
cmd := func() {
    fmt.Println(time.Now().Format("2006-01-02 15:04:05"))
}
rdbIndex := 0
cronClient, err := ddcron.NewCronClient(db, rdb, &rdbIndex, "TEST", "*/1 * * * * ?", cmd,
                                        ddcron.WithNodeName("11.98"),
                                        ddcron.WithDesc("测试定时任务"),
                                        //ddcron.WithSaveExecutionTime(true),
                                        ddcron.WithOneOff(true),
                                        //ddcron.WithHeartbeatRate(5),
                                        //ddcron.WithLog(log),
                                       )
if err != nil {
    panic(err)
}
```

启动定时任务

```go
// （本节点）
err = cronClient.Start()

// 全部节点
err = ddcron.PubChangeFuncStatus(rdb, cronClient.ID, true)
```

停止定时任务（本节点）

```go
// （本节点）
err = cronClient.Stop()

// 全部节点
err = ddcron.PubChangeFuncStatus(rdb, cronClient.ID, false)
```

修改定时任务频率（本节点）

```go
// （本节点）
err = cronClient.ChangeFuncSpec("*/3 * * * * ?")

// 全部节点
err = ddcron.PubChangeFuncSpec(rdb, cronClient.ID, "*/3 * * * * ?")
```




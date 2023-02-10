# go-ddcron
[![Goproxy.cn](https://goproxy.cn/stats/github.com/melf-xyzh/go-ddcron/badges/download-count.svg)](https://goproxy.cn)

### 简介

ddcron 是一个 Go语言分布式动态定时任务库

### 安装

```
go get -u github.com/melf-xyzh/go-ddcron
```

### 使用

```go
// 初始化Redis
rdb := redis.NewClient(&redis.Options{
    Addr:     "127.0.0.1:6379",
    Password: "",
    DB:       0,
})

// 初始化数据库链接
dsn := "root:123456789@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
if err!= nil {
    panic(err)
}

// 配置ddcron
cronConfig := ddcron.CronConfig{
    DB:  db,
    RDB: rdb,
}
// 设置需要执行的函数
cmd := func() {
    fmt.Println(time.Now().Format("2006-01-02 15:04:05"))
}
// 初始化定时任务
client, err := cronConfig.NewClient("127.0.0.1","test", "测试数据", "*/1 * * * * ?", cmd, true)
if err != nil {
    panic(err)
}
// 启动定时任务
err = cronConfig.PubChangeFuncStatus(client.ID, true)
if err != nil {
    panic(err)
}
time.Sleep(time.Second*5)
// 修改执行频率
err = cronConfig.PubChangeFuncSpec(client.ID, "*/2 * * * * ?")
if err != nil {
    panic(err)
}
time.Sleep(time.Second*5)
// 关闭定时任务
err = cronConfig.PubChangeFuncStatus(client.ID, false)
if err != nil {
    panic(err)
}
select {

}
```


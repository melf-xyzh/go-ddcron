/**
 * @Time    :2023/2/7 9:17
 * @Author  :Xiaoyu.Zhang
 */

package ddcron

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/melf-xyzh/go-ddcron/commons"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CronClient struct {
	ID                string
	db                *gorm.DB
	rdb               *redis.Client
	rdbIndex          int
	Log               *zap.Logger
	Name              string
	NodeName          string
	Desc              string
	spec              string
	tmpOff            bool
	oneOff            bool
	entryID           int
	status            bool
	heartbeatRate     int // 心跳频率（秒）
	heartbeat         func()
	cmd               func()
	task              func()
	cron              *cron.Cron // 定时任务
	SaveExecutionTime bool       // 保存执行时间
}

type ClientOption func(*CronClient)

var (
	onceDB0     commons.ErrOnce
	onceDB1     commons.ErrOnce
	channelOnce sync.Once
	cronMapOnce sync.Once
)
var (
	pubSubCronChange *redis.PubSub // Cron变化监听
	pubSubExp        *redis.PubSub // 键过期监听
	pubSubDel        *redis.PubSub // 键删除监听
)

var (
	cronNameMap map[string]*CronClient // map[cornName]*Client
	cronIdMap   map[string]*CronClient // map[ID]*Client
)

// NewCronClient
/**
 *  @Description: 创建一个CronClient
 *  @param db
 *  @param rdb
 *  @param rdbIndex
 *  @param name
 *  @param spec
 *  @param cmd
 *  @param opts
 *  @return client
 */
func NewCronClient(db *gorm.DB, rdb *redis.Client, rdbIndex *int, name, spec string, cmd func(), opts ...ClientOption) (client *CronClient, err error) {
	cronMapOnce.Do(func() {
		cronNameMap = make(map[string]*CronClient)
		cronIdMap = make(map[string]*CronClient)
	})
	client = &CronClient{
		ID:   commons.UUID(),
		Name: name,
		spec: spec,
		cmd:  cmd,
	}
	// 加载可选配置
	for _, opt := range opts {
		opt(client)
	}
	// 心跳频率
	if client.heartbeatRate == 0 {
		client.heartbeatRate = DefaultCronHeartbeatRate
	}
	// 初始化数据库
	err = client.initDB(db)
	if err != nil {
		return
	}
	// 初始化Redis
	err = client.initRDB(rdb, rdbIndex)
	if err != nil {
		return
	}
	// 避免重复创建
	clientByName, ok := cronNameMap[client.Name]
	if ok {
		client = clientByName
		cronIdMap[clientByName.ID] = client
		return
	}
	// 配置
	now := time.Now().Format("2006-01-02 15:04:05")
	cronConfig := CronConfig{
		ID:         client.ID,
		CreateTime: now,
		UpdateTime: now,
		NodeName:   client.NodeName,
		TmpOff:     client.tmpOff,
		OneOff:     client.oneOff,
		Name:       client.Name,
		Desc:       client.Desc,
		Spec:       client.spec,
		Status:     false,
	}
	// 初始化定时任务
	err = client.initCron()
	if err != nil {
		client.errLog("初始化定时任务错误", err)
		return
	}

	if client.tmpOff {
		err = client.db.Create(&cronConfig).Error
		if err != nil {
			return
		}
	} else {
		if client.oneOff {
			var status bool
			status, err = client.GetCronStatus()
			if err != nil {
				client.errLog("获取定时任务状态失败", err)
				return
			}
			if status {
				// 将数据库写入Map
				cronNameMap[client.Name] = client
				cronIdMap[client.ID] = client
				client.info("该定时任务已创建")
				return
			}
		}

		// 查询是否有修改过的执行频率
		var cronSpec CronSpec
		selectMap := make(map[string]interface{})
		selectMap["name"] = client.Name
		if !client.oneOff {
			selectMap["node_name"] = client.NodeName
		}
		err = client.db.Where(selectMap).First(&cronSpec).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			err = errors.New("数据库异常")
			return
		}
		// 非临时定时任务需要查询之前的频率
		// 再唠叨一下，事务一旦开始，你就应该使用 tx 处理数据
		tx := client.db.Begin()
		defer func() {
			if r := recover(); r != nil {
				tx.Rollback()
			}
		}()
		if err = tx.Error; err != nil {
			return
		}
		err = nil
		if cronSpec.ID == "" {
			// 创建默认频率记录
			cronSpec = CronSpec{
				ID:         commons.UUID(),
				CreateTime: now,
				NodeName:   client.NodeName,
				Name:       client.Name,
				Spec:       client.spec,
			}
			err = tx.Create(&cronSpec).Error
			if err != nil {
				tx.Rollback()
				return
			}
		}
		// 使用上次修改过的执行频率
		cronConfig.Spec = cronSpec.Spec
		// 将定时任务写入数据库
		err = tx.Create(&cronConfig).Error
		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit().Error
		if err == nil {
			// 将数据库写入Map
			cronNameMap[client.Name] = client
			cronIdMap[client.ID] = client
		}
	}
	return
}

// WithDesc
/**
 *  @Description: 设置任务描述
 *  @param description
 *  @return ClientOption
 */
func WithDesc(description string) ClientOption {
	return func(client *CronClient) {
		client.Desc = description
	}
}

// WithSaveExecutionTime
/**
 *  @Description: 设置任务描述
 *  @param description
 *  @return ClientOption
 */
func WithSaveExecutionTime(saveExecutionTime bool) ClientOption {
	return func(client *CronClient) {
		client.SaveExecutionTime = saveExecutionTime
	}
}

// WithLog
/**
 *  @Description: 设置Logger
 *  @param log
 *  @return ClientOption
 */
func WithLog(log *zap.Logger) ClientOption {
	return func(client *CronClient) {
		client.Log = log
	}
}

// WithNodeName
/**
 *  @Description: 设置节点名称
 *  @param nodeName
 *  @return ClientOption
 */
func WithNodeName(nodeName string) ClientOption {
	return func(client *CronClient) {
		client.NodeName = nodeName
		return
	}
}

// WithTmpOff
/**
 *  @Description: 设置是否临时定时任务
 *  @param tmpOff
 *  @return ClientOption
 */
func WithTmpOff(tmpOff bool) ClientOption {
	return func(client *CronClient) {
		client.tmpOff = tmpOff
	}
}

// WithOneOff
/**
 *  @Description: 设置是否单例定时任务(单节点)
 *  @param oneOff
 *  @return ClientOption
 */
func WithOneOff(oneOff bool) ClientOption {
	return func(client *CronClient) {
		client.oneOff = oneOff
	}
}

// WithHeartbeatRate
/**
 *  @Description: 设置心跳频率(保活机制)
 *  @param oneOff
 *  @return ClientOption
 */
func WithHeartbeatRate(heartbeatRate int) ClientOption {
	return func(client *CronClient) {
		client.heartbeatRate = heartbeatRate
	}
}

func (client *CronClient) errLog(msg string, err error) {
	msg = fmt.Sprintf("【ddcron】%s", msg)
	if client.Log != nil {
		client.Log.Error(msg, zap.Error(err))
	} else {
		log.Println(msg, err.Error())
	}
}

func (client *CronClient) info(msg string) {
	msg = fmt.Sprintf("【ddcron】%s", msg)
	if client.Log != nil {
		client.Log.Info(msg)
	} else {
		log.Println(msg)
	}
}

// initDB
/**
 *  @Description: 初始化数据库
 *  @receiver client
 *  @return err
 */
func (client *CronClient) initDB(db *gorm.DB) (err error) {
	if db == nil {
		err = errors.New("the db is nil")
		return
	}
	// 初始化数据表
	err = onceDB0.Do(func() error {
		err = db.AutoMigrate(
			CronConfig{},
			CronSpec{},
		)
		return err
	})
	if err != nil {
		return
	}
	// 将本节点定时任务全部删除
	err = onceDB1.Do(func() error {
		err = db.Where("node_name = ?", client.NodeName).Delete(&CronConfig{}).Error
		return err
	})
	if err != nil {
		return
	}
	client.db = db
	return
}

// initRDB
/**
 *  @Description: 初始化Redis
 *  @receiver client
 *  @return err
 */
func (client *CronClient) initRDB(rdb *redis.Client, rdbIndex *int) (err error) {
	if rdb == nil {
		err = errors.New("the rdb is nil")
		return
	}
	if rdbIndex == nil {
		err = errors.New("the rdbIndex is nil")
		return
	}
	client.rdb = rdb
	client.rdbIndex = *rdbIndex
	// 订阅
	if pubSubCronChange == nil {
		pubSubCronChange = client.rdb.Subscribe(context.Background(), CronChangeChannelName)
	}
	// 订阅键过期事件
	if pubSubExp == nil {
		// 开启键过期事件通知
		err = setNotifyKeyspaceEvents(client.rdb, DelConfig)
		if err != nil {
			err = errors.New("开启键过期事件通知失败：" + err.Error())
			return
		}
		// 监听此通道
		pubSubExp = client.rdb.Subscribe(context.Background(), fmt.Sprintf(DBKeyExpChannelName, strconv.Itoa(client.rdbIndex)))
	}
	// 订阅键删除事件
	if pubSubDel == nil {
		// 开启键删除事件通知
		err = setNotifyKeyspaceEvents(client.rdb, ExpConfig)
		if err != nil {
			err = errors.New("开启键删除事件通知失败：" + err.Error())
			return
		}
		pubSubDel = client.rdb.Subscribe(context.Background(), fmt.Sprintf(DBKeyDelChannelName, strconv.Itoa(client.rdbIndex)))
	}
	channelOnce.Do(func() {
		client.SubAndChangeFuncSpec()
	})
	return
}

// initCron
/**
 *  @Description: 初始化定时任务
 *  @receiver client
 *  @return err
 */
func (client *CronClient) initCron() (err error) {
	if client.cron == nil {
		client.cron = cron.New(cron.WithSeconds())
		// 添加键值心跳定时任务
		rate := client.heartbeatRate
		// 定义心跳
		client.heartbeat = func() {
			errRDB := client.rdb.SetEX(
				context.Background(),
				fmt.Sprintf(RdbCronHeartKey, client.NodeName, client.Name),
				time.Now().Format("2006-01-02 15:04:05"),
				time.Second*time.Duration(rate+DefaultCronHeartbeatWait),
			).Err()
			if errRDB != nil {
				client.errLog("心跳写入Redis失败", errRDB)
			}
		}
		// 添加心跳定时任务
		_, err = client.cron.AddFunc(fmt.Sprintf("*/%d * * * * ?", rate), client.heartbeat)
		if err != nil {
			client.errLog("创建心跳定时任务失败", err)
			err = errors.New("创建心跳定时任务失败")
			return
		}
		if client.SaveExecutionTime {
			client.task = func() {
				now := time.Now().Format("2006-01-02 15:04:05")
				client.info(fmt.Sprintf("定时任务【%s】【%s】开始执行！", client.Name, now))
				// 更新开始时间
				err = client.db.Model(&CronConfig{}).Select("*").
					Where("ID = ?", client.ID).Updates(map[string]interface{}{
					"update_time":     now,
					"last_start_time": now,
					"last_end_time":   "",
				}).Error
				if err != nil {
					client.errLog(fmt.Sprintf("更新定时任务【%s】失败！", client.Name), err)
				}
				// 执行任务
				client.cmd()

				now = time.Now().Format("2006-01-02 15:04:05")
				client.info(fmt.Sprintf("定时任务【%s】【%s】结束执行！", client.Name, now))
				// 更新结束时间
				err = client.db.Model(&CronConfig{}).Where("ID = ?", client.ID).Updates(map[string]interface{}{
					"update_time":   now,
					"last_end_time": now,
				}).Error
				if err != nil {
					client.errLog(fmt.Sprintf("更新定时任务【%s】失败！", client.Name), err)
				}
			}
		} else {
			client.task = client.cmd
		}
		var entryID cron.EntryID
		entryID, err = client.cron.AddFunc(client.spec, client.task)
		if err != nil {
			client.errLog("创建定时任务cmd失败", err)
			err = errors.New("创建定时任务cmd失败")
			return
		}
		client.entryID = int(entryID)
	}
	return
}

// SubAndChangeFuncSpec
/**
 *  @Description: 订阅并修改Spec
 *  @receiver client
 */
func (client *CronClient) SubAndChangeFuncSpec() {
	// 在另一个goroutine中进行订阅操作
	go func() {
		// 多通道监听
		for {
			select {
			case msg := <-pubSubCronChange.Channel():
				args := strings.Split(msg.Payload, ":")
				if len(args) >= 3 {
					id := args[1]
					cronClient, ok := cronIdMap[id]
					if ok {
						switch args[0] {
						case ChangeCronTag:
							cronClient.ChangeFuncSpec(args[2])
						case ChangeCronStatusTag:
							if args[2] == True {
								cronClient.Start()
							} else {
								cronClient.Stop()
							}
						}
					}
				}
			case msg := <-pubSubDel.Channel():
				args := strings.Split(msg.Payload, ":")
				if len(args) >= 3 && args[0] == CronHeartTag {
					client.CloseLoseCronData(args[1], args[2])
				}
			case msg := <-pubSubExp.Channel():
				args := strings.Split(msg.Payload, ":")
				if len(args) >= 3 && args[0] == CronHeartTag {
					client.CloseLoseCronData(args[1], args[2])
				}
			}
		}
	}()
}

// Start
/**
 *  @Description: 启动定时任务
 *  @receiver client
 *  @return err
 */
func (client *CronClient) Start() (err error) {
	var status bool
	status, err = client.GetCronStatus()
	if err != nil {
		client.errLog("获取定时任务状态失败", err)
		return
	}
	if status {
		client.info("该服务已在别处启动")
		return
	}
	var config CronConfig
	if client.oneOff {
		err = client.db.Where("name = ?", client.Name).First(&config).Error
	} else {
		err = client.db.Where("node_name = ?", client.NodeName).Where("name = ?", client.Name).First(&config).Error
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		client.errLog("数据库异常", err)
		return
	}
	if client.ID == "" {
		client.errLog("未查询到配置", err)
		return
	}
	if config.Spec != client.spec {
		err = client.ChangeFuncSpec(config.Spec)
		if err != nil {
			client.errLog("更新定时任务频率失败", err)
			return
		}
	}
	// 执行心跳函数，防止抖动过期！
	client.heartbeat()
	// 启动定时任务
	client.cron.Start()
	client.status = true
	// 更新数据库定时任务状态
	if client.oneOff {

		updateMap := map[string]interface{}{
			"node_name":   client.NodeName,
			"update_time": time.Now().Format("2006-01-02 15:04:05"),
			"status":      client.status,
			"entry_id":    client.entryID,
		}
		if config.ID != client.ID {
			updateMap["ID"] = client.ID
		}
		err = client.db.Model(&CronConfig{}).Where("name = ?", client.Name).Updates(updateMap).Error
	} else {
		err = client.db.Model(&CronConfig{}).Where("ID = ?", client.ID).Updates(map[string]interface{}{
			"update_time": time.Now().Format("2006-01-02 15:04:05"),
			"status":      client.status,
			"entry_id":    client.entryID,
		}).Error
	}
	if err != nil {
		client.errLog("更新数据库定时任务状态失败", err)
		client.cron.Stop()
		return
	}
	return
}

// Stop
/**
 *  @Description: 停止定时任务
 *  @receiver client
 *  @return err
 */
func (client *CronClient) Stop() (err error) {
	client.status = false
	// 更新数据库定时任务状态
	err = client.db.Model(&CronConfig{}).Where("ID = ?", client.ID).Updates(map[string]interface{}{
		"update_time": time.Now().Format("2006-01-02 15:04:05"),
		"status":      client.status,
	}).Error
	if err != nil {
		client.errLog("更新数据库定时任务状态失败", err)
		return
	}
	if client.cron != nil {
		client.cron.Stop()
	}
	return
}

// ChangeFuncSpec
/**
 *  @Description: 动态修改频率
 *  @receiver client
 */
func (client *CronClient) ChangeFuncSpec(spec string) (err error) {
	if client.tmpOff {
		err = errors.New("临时任务不支持修改执行频率")
		client.errLog("动态修改频率失败", err)
		return
	}
	oldEntryId := client.entryID
	var entryID cron.EntryID
	entryID, err = client.cron.AddFunc(spec, client.task)
	if err != nil {
		client.errLog("创建定时任务cmd失败", err)
		err = errors.New("创建定时任务cmd失败")
		return
	}
	client.entryID = int(entryID)
	client.spec = spec

	// 再唠叨一下，事务一旦开始，你就应该使用 tx 处理数据
	tx := client.db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	if err = tx.Error; err != nil {
		return err
	}
	// 更新数据库数据
	err = client.db.Model(&CronConfig{}).Where("ID = ?", client.ID).Updates(map[string]interface{}{
		"update_time": time.Now().Format("2006-01-02 15:04:05"),
		"entry_id":    client.entryID,
		"spec":        client.spec,
	}).Error
	if err != nil {
		client.errLog("更新数据库定时任务频率失败", err)
		tx.Rollback()
		return
	}
	// 更新默认频率
	err = tx.Model(&CronSpec{}).Where("node_name = ?", client.NodeName).Where("name = ?", client.Name).Updates(&CronSpec{
		UpdateTime: time.Now().Format("2006-01-02 15:04:05"),
		Spec:       client.spec,
	}).Error
	if err != nil {
		client.errLog("更新定时任务默认频率失败", err)
		tx.Rollback()
		return err
	}
	// 提交事务
	err = tx.Commit().Error
	if err != nil {
		client.errLog("更新定时任务频率失败", err)
		return
	} else {
		// 删除原有的
		client.cron.Remove(cron.EntryID(oldEntryId))
	}
	return
}

// GetCronStatus
/**
 *  @Description: 获取定时任务状态
 *  @receiver client
 *  @return status
 *  @return err
 */
func (client *CronClient) GetCronStatus() (status bool, err error) {
	var config CronConfig
	theDB := client.db
	if !client.oneOff {
		theDB = theDB.Where("node_name = ?", client.NodeName)
	}
	err = theDB.Where("name = ?", client.Name).First(&config).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		err = errors.New("数据库异常：" + err.Error())
		return
	}
	err = nil
	if config.ID != "" {
		// 等待键值过期
		time.Sleep(time.Second * DefaultCronHeartbeatWait)
		// 判断抢占的服务是否还活着
		key := fmt.Sprintf(RdbCronHeartKey, config.NodeName, client.Name)
		var result int64
		result, err = client.rdb.Exists(context.Background(), key).Result()
		if err != nil {
			err = errors.New("查询键是否存在异常：" + err.Error())
			return
		}
		if result > 0 {
			status = true
			return
		} else {
			// 该定时任务已停止
			status = false
			err = client.db.Model(&CronConfig{}).Where("ID = ?", config.ID).Updates(map[string]interface{}{
				"update_time": time.Now().Format("2006-01-02 15:04:05"),
				"status":      false,
			}).Error
			if err != nil {
				err = errors.New("更新定时任务状态失败：" + err.Error())
				return
			}
		}
	}
	return
}

// CloseLoseCronData
/**
 *  @Description: 移除失效的Cron数据
 *  @receiver client
 *  @param nodeName 节点名称
 *  @param name 定时任务名称
 *  @param status
 *  @return err
 */
func (client *CronClient) CloseLoseCronData(nodeName, name string) (err error) {
	//// 查询该定时任务
	//err = client.db.Model(&CronConfig{}).Where("node_name = ?", nodeName).Where("name = ?", name).Updates(map[string]interface{}{
	//	"update_time": time.Now().Format("2006-01-02 15:04:05"),
	//	"status":      false,
	//}).Error
	//if err != nil {
	//	err = errors.New("数据库异常：" + err.Error())
	//	return
	//}
	var config CronConfig
	// 查询该定时任务
	err = client.db.Model(&CronConfig{}).Where("node_name = ?", nodeName).Where("name = ?", name).First(&config).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		err = errors.New("数据库异常：" + err.Error())
		return
	}
	if config.Status == false {
		// 被手动关停，无需启动
		return
	}
	// 接替启动
	clientCron, ok := cronNameMap[name]
	if ok {
		client.info("正在接替启动")
		clientCron.Start()
	}
	return
}

// PubChangeFuncStatus
/**
 *  @Description: 启动/停止定时任务
 *  @param rdb
 *  @param ID
 *  @param status
 *  @return err
 */
func PubChangeFuncStatus(rdb *redis.Client, id string, status bool) (err error) {
	if status {
		// 发布消息
		rdb.Publish(context.Background(), CronChangeChannelName, fmt.Sprintf("%s:%s:%s", ChangeCronStatusTag, id, True))
	} else {
		// 发布消息
		rdb.Publish(context.Background(), CronChangeChannelName, fmt.Sprintf("%s:%s:%s", ChangeCronStatusTag, id, False))
	}
	return
}

// PubChangeFuncSpec
/**
 *  @Description: 动态修改定时任务执行频率
 *  @param rdb
 *  @param ID
 *  @param spec
 *  @return err
 */
func PubChangeFuncSpec(rdb *redis.Client, id, spec string) (err error) {
	// 发布消息
	rdb.Publish(context.Background(), CronChangeChannelName, fmt.Sprintf("%s:%s:%s", ChangeCronTag, id, spec))
	return
}

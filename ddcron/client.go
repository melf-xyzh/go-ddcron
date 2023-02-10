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
	"gorm.io/gorm"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	CronChangeChannelName = "cronchange"
	DBKeyExpChannelName   = "__keyevent@%s__:expired" // 键过期通道名
	DBKeyDelChannelName   = "__keyevent@%s__:del"     // 键删除通道名
)

const (
	// RdbCronHeartKey 定时任务心跳 CronHB:节点名称:定时任务名称
	RdbCronHeartKey = CronHeartTag + ":%s:%s"
	// DefaultCronHeartbeatRate 默认定时任务心跳频率
	DefaultCronHeartbeatRate = 5
	// DefaultCronHeartbeatWait 定时任务心跳等待
	DefaultCronHeartbeatWait = 1
)

const (
	True  = "true"
	False = "false"
)

const (
	ChangeCronTag       = "CC"
	ChangeCronStatusTag = "SC"
	CronHeartTag        = "CronHB"
)

const (
	notifyKeyspaceEvents = "notify-keyspace-events" // 键空间通知功能配置键
	ExpConfig            = "Ex"                     // 开启过期通知
	DelConfig            = "Eg"                     // 开启删除通知
)

type CronConfig struct {
	DB            *gorm.DB      // 数据库连接池
	RDB           *redis.Client // Redis连接池
	RDBIndex      *int          // Redis数据库索引
	HeartbeatRate int           // 心跳频率（秒）
}

var (
	cronMap          map[string]CronClient
	cronNameMap      map[string]struct{}
	pubSubCronChange *redis.PubSub // Cron变化监听
	pubSubExp        *redis.PubSub // 键过期监听
	pubSubDel        *redis.PubSub // 键删除监听
)

var (
	ctx         = context.Background()
	onceChannel = sync.Once{}
	onceDB      = sync.Once{}
)

// setNotifyKeyspaceEvents
/**
 *  @Description: 设置事件通知配置
 *  @param rdb
 *  @param value
 *  @return err
 */
func setNotifyKeyspaceEvents(rdb *redis.Client, value string) (err error) {
	// 获取事件通知配置
	var oldValue string
	oldValue, err = getNotifyKeyspaceEvents(rdb)
	if err != nil {
		err = errors.New("获取事件通知配置失败：" + err.Error())
		return
	}
	newValue := commons.StrUnique(oldValue + value)
	// 开启键过期事件通知
	err = rdb.ConfigSet(ctx, notifyKeyspaceEvents, newValue).Err()
	if err != nil {
		return
	}
	return
}

// getNotifyKeyspaceEvents
/**
 *  @Description: 获取事件通知配置
 *  @param rdb
 *  @return value
 *  @return err
 */
func getNotifyKeyspaceEvents(rdb *redis.Client) (value string, err error) {
	res := rdb.ConfigGet(ctx, notifyKeyspaceEvents)
	if res.Err() != nil {
		err = res.Err()
		return
	}
	value = res.Val()[1].(string)
	return
}

// NewClient
/**
 *  @Description: 创建一个定时任务
 *  @receiver cronConfig
 *  @param name
 *  @param desc
 *  @param spec
 *  @param cmd
 *  @param oneOff
 *  @return cronClient
 *  @return err
 */
func (cronConfig CronConfig) NewClient(nodeName, name, desc, spec string, cmd func(), oneOff, tmpOff bool) (cronClient CronClient, err error) {
	if cronConfig.DB == nil {
		panic("请指定数据库")
	}
	if cronConfig.RDB == nil {
		panic("请指定Redis")
	}
	if cronConfig.RDBIndex == nil {
		panic("请指定RDB索引")
	}
	if cronConfig.HeartbeatRate == 0 {
		cronConfig.HeartbeatRate = DefaultCronHeartbeatRate
	}
	onceDB.Do(func() {
		err = cronConfig.DB.AutoMigrate(&CronClient{}, &CronSpec{})
		if err != nil {
			return
		}
		// 删除本节点所有定时任务
		err = cronConfig.DB.Where("node_name = ?", nodeName).Delete(&CronClient{}).Error
		if err != nil {
			return
		}
	})
	if cronMap == nil {
		cronMap = make(map[string]CronClient)
	}
	if cronNameMap == nil {
		cronNameMap = make(map[string]struct{})
	}
	// 订阅
	if pubSubCronChange == nil {
		pubSubCronChange = cronConfig.RDB.Subscribe(ctx, CronChangeChannelName)
	}
	// 订阅键过期事件
	if pubSubExp == nil {
		// 开启键过期事件通知
		err = setNotifyKeyspaceEvents(cronConfig.RDB, DelConfig)
		if err != nil {
			err = errors.New("开启键过期事件通知失败：" + err.Error())
			return
		}
		// 监听此通道
		pubSubExp = cronConfig.RDB.Subscribe(ctx, fmt.Sprintf(DBKeyExpChannelName, strconv.Itoa(*cronConfig.RDBIndex)))
	}
	// 订阅键删除事件
	if pubSubDel == nil {
		// 开启键删除事件通知
		err = setNotifyKeyspaceEvents(cronConfig.RDB, ExpConfig)
		if err != nil {
			err = errors.New("开启键删除事件通知失败：" + err.Error())
			return
		}
		pubSubDel = cronConfig.RDB.Subscribe(ctx, fmt.Sprintf(DBKeyDelChannelName, strconv.Itoa(*cronConfig.RDBIndex)))
	}
	onceChannel.Do(func() {
		cronConfig.SubAndChangeFuncSpec()
	})

	// 若为单例定时任务，查询是否被强占
	if oneOff {
		var cc CronClient
		err = cronConfig.DB.Where("name = ?", name).First(&cc).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			err = errors.New("数据库异常：" + err.Error())
			return
		}
		if cc.ID != "" {
			// 等待键值过期
			time.Sleep(time.Second * DefaultCronHeartbeatWait)
			// 判断强占的服务是否还活着
			key := fmt.Sprintf(RdbCronHeartKey, cc.NodeName, name)
			var result int64
			result, err = cronConfig.RDB.Exists(ctx, key).Result()
			if err != nil {
				err = errors.New("查询键是否存在异常：" + err.Error())
				return
			}
			if result > 0 {
				err = errors.New(fmt.Sprintf("该定时任务 节点：【%s】名称：【%s】已存在，请勿重复创建", cc.NodeName, name))
				return
			} else {
				err = cronConfig.DeleteLoseCronData(cc.NodeName, name, nil)
				if err != nil {
					err = errors.New("删除过期Cron失败：" + err.Error())
					return
				}
			}
			//err = errors.New("该定时任务已存在，请勿重复创建")
			//return
		}
		err = nil
	}
	// 避免重复启用
	_, ok := cronNameMap[name]
	if ok {
		return
	}
	cronClient = CronClient{
		ID:         commons.UUID(),
		CreateTime: time.Now().Format("2006-01-02 15:04:05"),
		OneOff:     oneOff,
		TmpOff:     tmpOff,
		NodeName:   nodeName,
		Name:       name,
		Desc:       desc,
		Status:     false,
		Cron:       cron.New(cron.WithSeconds()),
		Spec:       spec,
	}
	// 添加键值心跳定时任务
	rate := cronConfig.HeartbeatRate
	// 定义心跳
	cronClient.Heartbeat = func() {
		errRDB := cronConfig.RDB.SetEX(ctx, fmt.Sprintf(RdbCronHeartKey, cronClient.NodeName, cronClient.Name), time.Now().Format("2006-01-02 15:04:05"), time.Second*time.Duration(rate+DefaultCronHeartbeatWait)).Err()
		if errRDB != nil {
			err = errors.New("Cron心跳写入Redis失败：" + err.Error())
			return
		}
	}
	// 添加心跳定时任务
	_, err = cronClient.Cron.AddFunc(fmt.Sprintf("*/%d * * * * ?", rate), cronClient.Heartbeat)

	if err != nil {
		err = errors.New("创建Cron心跳定时任务失败：" + err.Error())
		return
	}

	task := func() {
		now := time.Now().Format("2006-01-02 15:04:05")
		log.Println(fmt.Sprintf("定时任务【%s】开始执行！", name))
		err = cronConfig.DB.Model(&CronClient{}).Select("*").Where("id = ?", cronClient.ID).Updates(map[string]interface{}{
			"update_time":     now,
			"last_start_time": now,
			"last_end_time":   "",
		}).Error
		if err != nil {
			log.Println("更新定时任务失败")
		}
		// 执行任务
		cmd()

		log.Println(fmt.Sprintf("定时任务【%s】结束执行！", name))

		now = time.Now().Format("2006-01-02 15:04:05")
		// 更新数据库
		err = cronConfig.DB.Model(&CronClient{}).Select("*").Where("id = ?", cronClient.ID).Updates(map[string]interface{}{
			"update_time":   now,
			"last_end_time": now,
		}).Error
		if err != nil {
			log.Println("更新定时任务失败")
		}
	}
	var entryID cron.EntryID
	entryID, err = cronClient.Cron.AddFunc(spec, task)
	if err != nil {
		return
	}
	cronClient.Cmd = cmd
	cronClient.EntryID = int(entryID)

	// 临时定时任务直接创建
	if tmpOff {
		// 将定时任务写入数据库
		err = cronConfig.DB.Create(&cronClient).Error
		if err != nil {
			return
		}
	} else {
		// 非临时定时任务需要查询之前的频率
		// 查询是否有修改过的执行频率
		var cronSpec CronSpec
		if cronClient.OneOff {
			err = cronConfig.DB.Where("name = ?", name).First(&cronSpec).Error
		} else {
			err = cronConfig.DB.Where("node_name = ?", nodeName).Where("name = ?", name).First(&cronSpec).Error
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			err = errors.New("数据库异常")
			return
		}
		err = nil
		if cronSpec.ID != "" {
			// 使用上次修改过的执行频率
			cronClient.Spec = cronSpec.Spec
		}
		// 再唠叨一下，事务一旦开始，你就应该使用 tx 处理数据
		tx := cronConfig.DB.Begin()
		defer func() {
			if r := recover(); r != nil {
				tx.Rollback()
			}
		}()
		if err = tx.Error; err != nil {
			return
		}
		// 将定时任务写入数据库
		err = tx.Create(&cronClient).Error
		if err != nil {
			tx.Rollback()
			return
		}
		// 查询是否存在默认频率
		var cronSpec0 CronSpec
		if cronClient.OneOff {
			err = tx.Where("name = ?", name).First(&cronSpec0).Error
		} else {
			err = tx.Where("node_name = ?", nodeName).Where("name = ?", name).First(&cronSpec0).Error
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			tx.Rollback()
			return
		}
		if cronSpec0.ID == "" {
			// 创建默认频率记录
			cronSpec0 = CronSpec{
				ID:         commons.UUID(),
				CreateTime: time.Now().Format("2006-01-02 15:04:05"),
				NodeName:   nodeName,
				Name:       name,
				Spec:       spec,
			}
			err = tx.Create(&cronSpec0).Error
		} else {
			// 更新默认频率
			err = tx.Model(&CronSpec{}).Where("id = ?", cronSpec0.ID).Updates(map[string]interface{}{
				"update_time": time.Now().Format("2006-01-02 15:04:05"),
				"node_name":   cronClient.NodeName,
				"spec":        spec,
			}).Error
		}
		if err != nil {
			tx.Rollback()
			return
		}
		// 将数据写入数据库
		err = tx.Commit().Error
		if err != nil {
			return
		} else {
			// 将数据库写入Map
			cronMap[cronClient.ID] = cronClient
			cronNameMap[cronClient.Name] = struct{}{}
		}
	}
	return
}

// SubAndChangeFuncSpec
/**
 *  @Description: 订阅并修改Spec
 */
func (cronConfig *CronConfig) SubAndChangeFuncSpec() {
	// 在另一个goroutine中进行订阅操作
	go func() {
		openStatus := true
		// 多通道监听
		for {
			select {
			case msg := <-pubSubCronChange.Channel():
				args := strings.Split(msg.Payload, ":")
				if len(args) >= 3 {
					switch args[0] {
					case ChangeCronTag:
						cronConfig.ChangeFuncSpec(args[1], args[2])
					case ChangeCronStatusTag:
						if args[2] == True {
							cronConfig.ChangeCornStatus(args[1], true)
						} else {
							cronConfig.ChangeCornStatus(args[1], false)
						}
					}
				}
			case msg := <-pubSubDel.Channel():
				args := strings.Split(msg.Payload, ":")
				if len(args) >= 3 && args[0] == CronHeartTag {
					cronConfig.DeleteLoseCronData(args[1], args[2], &openStatus)
				}
			case msg := <-pubSubExp.Channel():
				args := strings.Split(msg.Payload, ":")
				if len(args) >= 3 && args[0] == CronHeartTag {
					cronConfig.DeleteLoseCronData(args[1], args[2], &openStatus)
				}
			}
		}
	}()
}

// DeleteLoseCronData
/**
 *  @Description: 移除失效的Cron数据
 *  @receiver cronConfig
 *  @param nodeName 节点名称
 *  @param name 定时任务名称
 *  @return err
 */
func (cronConfig *CronConfig) DeleteLoseCronData(nodeName, name string, status *bool) (err error) {
	// 查询该定时任务
	var cc CronClient
	if status != nil {
		err = cronConfig.DB.Where("node_name = ? AND name = ? AND status = ?", nodeName, name, *status).First(&cc).Error
	} else {
		err = cronConfig.DB.Where("node_name = ? AND name = ?", nodeName, name).First(&cc).Error
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		err = errors.New("数据库异常：" + err.Error())
		return
	}
	if cc.ID != "" {
		err = cronConfig.DB.Where("id = ?", cc.ID).Delete(&CronClient{}).Error
		if err != nil {
			err = errors.New("数据库异常：" + err.Error())
			return
		}
	}
	return
}

// PubChangeFuncSpec
/**
 *  @Description: 动态修改定时任务执行频率
 *  @receiver cronConfig
 *  @param id
 *  @param spec
 *  @return err
 */
func (cronConfig *CronConfig) PubChangeFuncSpec(id string, spec string) (err error) {
	// 发布消息
	cronConfig.RDB.Publish(ctx, CronChangeChannelName, fmt.Sprintf("%s:%s:%s", ChangeCronTag, id, spec))
	return
}

// PubChangeFuncStatus
/**
 *  @Description: 启动/停止定时任务
 *  @receiver cronConfig
 *  @param id
 *  @param status
 *  @return err
 */
func (cronConfig *CronConfig) PubChangeFuncStatus(id string, status bool) (err error) {
	if status {
		// 发布消息
		cronConfig.RDB.Publish(ctx, CronChangeChannelName, fmt.Sprintf("%s:%s:%s", ChangeCronStatusTag, id, True))
	} else {
		// 发布消息
		cronConfig.RDB.Publish(ctx, CronChangeChannelName, fmt.Sprintf("%s:%s:%s", ChangeCronStatusTag, id, False))
	}
	return
}

// ChangeFuncSpec
/**
 *  @Description: 动态修改定时任务执行频率
 *  @receiver cronConfig
 *  @param id
 *  @param spec
 *  @return err
 */
func (cronConfig *CronConfig) ChangeFuncSpec(id string, spec string) (err error) {
	if cronMap == nil {
		cronMap = make(map[string]CronClient)
	}
	client, ok := cronMap[id]
	if !ok {
		// 不存在则无需修改
		return
	}
	if client.TmpOff {
		// 临时任务不支持修改执行频率
		return
	}
	oldEntryId := client.EntryID
	// 先创建新的，则删除原有的
	newEntryId, err := client.Cron.AddFunc(spec, client.Cmd)
	if err != nil {
		return
	}
	// 再唠叨一下，事务一旦开始，你就应该使用 tx 处理数据
	tx := cronConfig.DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	if err = tx.Error; err != nil {
		return err
	}
	// 更新数据库数据
	err = tx.Model(&CronClient{}).Where("id = ?", client.ID).Updates(map[string]interface{}{
		"update_time": time.Now().Format("2006-01-02 15:04:05"),
		"entry_id":    int(newEntryId),
		"spec":        spec,
	}).Error
	if err != nil {
		tx.Rollback()
		return err
	}
	// 更新默认频率
	err = tx.Model(&CronSpec{}).Where("node_name = ?", client.NodeName).Where("name = ?", client.Name).Updates(map[string]interface{}{
		"update_time": time.Now().Format("2006-01-02 15:04:05"),
		"spec":        spec,
	}).Error
	if err != nil {
		tx.Rollback()
		return err
	}
	// 提交事务
	err = tx.Commit().Error
	if err != nil {
		return
	} else {
		// 将新的Entry写入内存
		client.EntryID = int(newEntryId)
		client.Spec = spec
		cronMap[id] = client
	}
	// 删除原有的
	client.Cron.Remove(cron.EntryID(oldEntryId))
	return
}

// ChangeCornStatus
/**
 *  @Description: 启动/停止定时任务
 *  @receiver cronConfig
 *  @param id
 *  @param status
 *  @return err
 */
func (cronConfig *CronConfig) ChangeCornStatus(id string, status bool) (err error) {
	if cronMap == nil {
		cronMap = make(map[string]CronClient)
	}
	client, ok := cronMap[id]
	if !ok {
		// 不存在则无需修改
		return
	}
	// 启动定时任务
	if status {
		// 执行心跳函数，防止抖动过期！
		client.Heartbeat()
		// 启动定时任务
		client.Cron.Start()
		// 更新数据库数据
		err = cronConfig.DB.Model(&CronClient{}).Where("id = ?", client.ID).Updates(map[string]interface{}{
			"update_time": time.Now().Format("2006-01-02 15:04:05"),
			"status":      true,
		}).Error
		if err != nil {
			client.Cron.Stop()
			return
		} else {
			// 将新的Entry写入内存
			client.Status = status
			cronMap[id] = client
		}
	} else {
		// 停止定时任务
		// 更新数据库数据
		err = cronConfig.DB.Model(&CronClient{}).Where("id = ?", client.ID).Updates(map[string]interface{}{
			"update_time": time.Now().Format("2006-01-02 15:04:05"),
			"status":      false,
		}).Error
		if err != nil {
			// 更新失败，不进行操作
			return
		} else {
			// 停止定时任务
			client.Cron.Stop()
			// 将新的Entry写入内存
			client.Status = status
			cronMap[id] = client
		}
	}
	return
}

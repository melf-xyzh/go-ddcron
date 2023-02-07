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
	"strings"
	"sync"
	"time"
)

const (
	CronChangeChannelName = "cronchange"
	ChangeCronTag         = "CC"
	ChangeCronStatusTag   = "SC"
	True                  = "true"
	False                 = "false"
)

type CronConfig struct {
	DB  *gorm.DB
	RDB *redis.Client
}

var (
	cronMap     map[string]CronClient
	cronNameMap map[string]struct{}
	pubSub      *redis.PubSub
)

var (
	ctx         = context.Background()
	onceChannel = sync.Once{}
	onceDB      = sync.Once{}
)

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
func (cronConfig CronConfig) NewClient(nodeName, name, desc, spec string, cmd func(), oneOff bool) (cronClient CronClient, err error) {
	if cronConfig.DB == nil {
		panic("请指定数据库")
	}
	if cronConfig.RDB == nil {
		panic("请指定Redis")
	}
	onceDB.Do(func() {
		err = cronConfig.DB.AutoMigrate(&CronClient{},&CronSpec{})
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
	if pubSub == nil {
		pubSub = cronConfig.RDB.Subscribe(ctx, CronChangeChannelName)
		onceChannel.Do(func() {
			cronConfig.SubAndChangeFuncSpec()
		})
	}
	// 若为单例定时任务，查询是否被强占
	if oneOff {
		var cc CronClient
		err = cronConfig.DB.Where("name = ?", name).First(&cc).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return
		}
		if cc.ID != "" {
			err = errors.New("该定时任务已存在，请勿重复创建")
			return
		}
		err = nil
	}
	// 避免重复启用
	_, ok := cronNameMap[name]
	if ok {
		return
	}
	// 查询是否有修改过的执行频率
	var cronSpec CronSpec
	err = cronConfig.DB.Where("node_name = ?", nodeName).Where("name = ?", name).First(&cronSpec).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return
	}
	err = nil
	if cronSpec.ID != "" {
		// 使用上次修改过的执行频率
		spec = cronSpec.Spec
	}
	cronClient = CronClient{
		ID:         commons.UUID(),
		CreateTime: time.Now().Format("2006-01-02 15:04:05"),
		OneOff:     oneOff,
		NodeName:   nodeName,
		Name:       name,
		Desc:       desc,
		Status:     false,
		Cron:       cron.New(cron.WithSeconds()),
	}
	var entryID cron.EntryID
	entryID, err = cronClient.Cron.AddFunc(spec, cmd)
	if err != nil {
		return
	}
	cronClient.Spec = spec
	cronClient.Cmd = cmd
	cronClient.EntryID = int(entryID)

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
	err = tx.Where("node_name = ?", nodeName).Where("name = ?", name).First(&cronSpec0).Error
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
	return
}

// SubAndChangeFuncSpec
/**
 *  @Description: 订阅并修改Spec
 */
func (cronConfig *CronConfig) SubAndChangeFuncSpec() {
	// 在另一个goroutine中进行订阅操作
	ch := pubSub.Channel()
	go func() {
		for msg := range ch {
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
		}
	}()
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

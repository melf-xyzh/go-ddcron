/**
 * @Time    :2023/7/9 16:20
 * @Author  :MELF晓宇
 * @FileName:rdb.go
 * @Project :go-ddcron
 * @Software:GoLand
 * @Information:
 *
 */

package ddcron

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/melf-xyzh/go-ddcron/commons"
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
	err = rdb.ConfigSet(context.Background(), notifyKeyspaceEvents, newValue).Err()
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
	res := rdb.ConfigGet(context.Background(), notifyKeyspaceEvents)
	if res.Err() != nil {
		err = res.Err()
		return
	}
	value = res.Val()[1].(string)
	return
}

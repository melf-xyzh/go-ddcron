/**
 * @Time    :2023/7/9 15:10
 * @Author  :MELF晓宇
 * @FileName:constant.go
 * @Project :go-ddcron
 * @Software:GoLand
 * @Information:
 *
 */

package ddcron

const (
	CronChangeChannelName = "cronchange"
	DBKeyExpChannelName   = "__keyevent@%s__:expired" // 键过期通道名
	DBKeyDelChannelName   = "__keyevent@%s__:del"     // 键删除通道名
)

const (
	notifyKeyspaceEvents = "notify-keyspace-events" // 键空间通知功能配置键
	ExpConfig            = "Ex"                     // 开启过期通知
	DelConfig            = "Eg"                     // 开启删除通知
)

const (
	ChangeCronTag       = "CC"
	ChangeCronStatusTag = "SC"
	CronHeartTag        = "CronHB"
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

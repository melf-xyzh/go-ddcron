/**
 * @Time    :2023/2/7 10:07
 * @Author  :Xiaoyu.Zhang
 */

package ddcron

import "github.com/robfig/cron/v3"

type CronClient struct {
	ID         string     `json:"id"                 gorm:"column:id;primary_key;type:varchar(36)"`
	CreateTime string     `json:"createTime"         gorm:"column:create_time;type:varchar(20);index;"`
	UpdateTime string     `json:"updateTime"         gorm:"column:update_time;type:varchar(20);"`
	NodeName   string     `json:"nodeName"           gorm:"column:node_name;type:varchar(50);index;"`         // 节点名称
	OneOff     bool       `json:"oneOff"             gorm:"column:one_off;comment:单例开关;index;"`               // 单例开关
	Name       string     `json:"name"               gorm:"column:name;comment:任务名称;index;type:varchar(50);"` // 任务名称
	Desc       string     `json:"desc"               gorm:"column:desc;comment:任务描述;type:text;"`              // 任务描述
	Spec       string     `json:"spec"               gorm:"column:spec;comment:Cron表达式;type:varchar(50);"`    // Cron表达式
	EntryID    int        `json:"entryId"            gorm:"column:entry_id;comment:entryId;index;"`           // entryId
	Status     bool       `json:"status"             gorm:"column:status;comment:任务状态;default:false"`         // 状态
	Cron       *cron.Cron `json:"-"                  gorm:"-"`                                                // 定时任务管理器
	Cmd        func()     `json:"-"                  gorm:"-"`                                                // 定时任务函数
}

// TableName CronClient 表名
func (CronClient) TableName() string {
	return "cron"
}

// CronSpec Cron执行频率
type CronSpec struct {
	ID         string `json:"id"                 gorm:"column:id;primary_key;type:varchar(36)"`
	CreateTime string `json:"createTime"         gorm:"column:create_time;type:varchar(20);index;"`
	UpdateTime string `json:"updateTime"         gorm:"column:update_time;type:varchar(20);"`
	NodeName   string `json:"nodeName"           gorm:"column:node_name;type:varchar(50);index;"`         // 节点名称
	Name       string `json:"name"               gorm:"column:name;comment:任务名称;index;type:varchar(50);"` // 任务名称
	Spec       string `json:"spec"               gorm:"column:spec;comment:Cron表达式;type:varchar(50);"`    // Cron表达式
}

// TableName CronSpec 表名
func (CronSpec) TableName() string {
	return "cron_spec"
}

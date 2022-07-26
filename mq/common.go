package mq

import (
	"time"
)

const (
	QUEUE_WAITING = "queue:waiting:"
	QUEUE_DELAYED = "queue:delayed"
	QUEUE_FAILD   = "queue:failed"
)

type JobHandler func(client *Client, data string) bool

type FailHandler func(client *Client, item JobItem) bool

type Job struct {
	Queue  string
	Handle JobHandler
}

type JobItem struct {
	Queue    string `json:"queue"`    //队列名称
	Attempts int    `json:"attempts"` //尝试次数
	Data     string `json:"data"`
}

func NewJobItem(Queue string, Data string) JobItem {
	return JobItem{
		Queue:    QUEUE_WAITING + Queue,
		Attempts: 0,
		Data:     Data,
	}
}

type Option struct {
	Jobs        []Job         //队列实例
	RetrySecond time.Duration // 重试间隔
	MaxAttempts int           // 最大重试次数
	Process     int           // 消费者数量
}

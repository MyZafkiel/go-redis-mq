package mq

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-redis/redis/v8"
	"runtime"
	"strconv"
	"time"
)

type Mq struct {
	Option
	timer  *time.Ticker
	Rdb    *redis.Client //redis客户端
	ctx    context.Context
	fail   FailHandler
	Client *Client
}

func NewMq(rdb *redis.Client, option *Option) *Mq {
	mq := &Mq{
		Option: *option,
		Rdb:    rdb,
		ctx:    context.Background(),
		Client: NewClient(rdb),
	}
	if mq.Process == 0 {
		mq.Process = runtime.NumCPU() * 2
	}
	if mq.MaxAttempts == 0 {
		mq.MaxAttempts = 3
	}
	if mq.RetrySecond == 0 {
		mq.RetrySecond = time.Second * 5
	}
	return mq
}

func (m *Mq) RegFailQueueHandle() {

}

func (m *Mq) Run(asyn bool, d time.Duration) error {
	if m.timer != nil {
		return errors.New("mq is running")
	}
	if m.Process < 0 {
		return errors.New("process must be greater than 0")
	}
	if len(m.Jobs) == 0 {
		return errors.New("consumers is empty")
	}
	_, err := m.Rdb.Ping(m.ctx).Result()
	if err != nil {
		return err
	}
	for i := 0; i < m.Process; i++ {
		go consumer(m)
	}
	if asyn {
		go m.delayQueue(d)
	} else {
		m.delayQueue(d)
	}
	return nil
}

func consumer(mq *Mq) {
	Queues := make([]string, 0, len(mq.Jobs))
	Handle := map[string]JobHandler{}
	for _, v := range mq.Jobs {
		tmp := QUEUE_WAITING + v.Queue
		Queues = append(Queues, tmp)
		Handle[tmp] = v.Handle
	}
	for {
		res := mq.Rdb.BRPop(mq.ctx, time.Second*5, Queues...)
		if res.Err() != nil {
			continue
		}
		val := res.Val()
		mq.event(Handle[val[0]], val[1])
	}
}

func (m *Mq) event(handle JobHandler, r string) {
	if handle == nil {
		//fmt.Println("handle is nil")
		return
	}
	var data JobItem
	err := json.Unmarshal([]byte(r), &data)
	if err != nil {
		return
	}
	defer func(data JobItem) {
		if recover() != nil {
			data.Attempts++
			if data.Attempts > m.MaxAttempts {
				if m.Client.fail(data) != nil {
					return
				}
			} else {
				if m.Client.delay(data, m.RetrySecond) != nil {
					return
				}
			}
		}
	}(data)
	if !handle(m.Client, data.Data) {
		data.Attempts++
		if data.Attempts > m.MaxAttempts {
			m.Client.fail(data)
		} else {
			m.Client.delay(data, m.RetrySecond)
		}
	}
}

func (m *Mq) delayQueue(d time.Duration) {
	m.timer = time.NewTicker(d)
	for {
		select {
		case <-m.timer.C:
			m.delayHandle()
		}
	}
}

func (m *Mq) delayHandle() {
	data := m.Rdb.ZRangeByScoreWithScores(m.ctx, QUEUE_DELAYED, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(time.Now().Unix(), 10),
		Count: 128,
	}).Val()
	member := make([]interface{}, 0, len(data))
	for _, v := range data {
		member = append(member, v.Member)
	}
	m.Rdb.ZRem(m.ctx, QUEUE_DELAYED, member...)
	for _, v := range data {
		var da JobItem
		err := json.Unmarshal([]byte(v.Member.(string)), &da)
		if err != nil {
			continue
		}
		m.delayPush(da)
	}
}

func (m *Mq) delayPush(data JobItem) {
	for i := 0; i < 5; i++ {
		if m.Client.add(data) == nil {
			return
		}
	}
	if m.Client.fail(data) != nil {
		return
	}
}

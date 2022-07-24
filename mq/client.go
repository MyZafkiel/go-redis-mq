package mq

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"time"
)

type Client struct {
	Rdb *redis.Client
	Ctx context.Context
}

func NewClient(rdb *redis.Client) *Client {
	return &Client{
		Rdb: rdb,
		Ctx: context.Background(),
	}
}

func (c *Client) Send(queue string, data interface{}, delay time.Duration) error {
	job := NewJobItem(queue, data)
	if delay < 1 {
		return c.add(job)
	}
	return c.delay(job, delay)
}

func (c *Client) add(item JobItem) error {
	js, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return c.Rdb.LPush(c.Ctx, item.Queue, js).Err()
}

func (c *Client) delay(item JobItem, delay time.Duration) error {
	js, err := json.Marshal(item)
	if err != nil {
		return err
	}
	for i := 0; i < 5; i++ {
		if err = c.Rdb.ZAdd(c.Ctx, QUEUE_DELAYED, &redis.Z{
			Score:  float64(time.Now().Add(delay).Unix()),
			Member: js,
		}).Err(); err == nil {
			return nil
		}
	}
	return err
}

func (c *Client) fail(item JobItem) error {
	js, err := json.Marshal(item)
	if err != nil {
		return err
	}
	for i := 0; i < 5; i++ {
		if err = c.Rdb.LPush(c.Ctx, QUEUE_FAILD, js).Err(); err == nil {
			return nil
		}
	}
	return err
}

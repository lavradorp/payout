package data

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

func InitRedisClient() *redis.Client {
	var rdb *redis.Client

	rdb = redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "",
		DB:       0,
	})

	return rdb
}

func GetRedisData(ctx context.Context, rdb *redis.Client, pattern string) (string, error){
	data, err := rdb.Get(ctx, pattern).Result()
	fmt.Print(data)

	if err != nil {
		return "", err
	}

	return data, nil
}

func ScanAndGetRedisData(ctx context.Context, rdb *redis.Client, pattern string) (map[string]string, error) {
	var keys []string

	iter := rdb.Scan(ctx, 0, pattern, 0).Iterator()

	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
		
	if err := iter.Err(); err != nil {
		return nil, err
	}

	result := make(map[string]string)

	if len(keys) == 0{
		return result, nil
	}

	values, err := rdb.MGet(ctx, keys...).Result()

	if err != nil{
		return nil, err
	}

	for i, key := range keys{
		if values[i] != nil {
			result[key] = values[i].(string)
		} else {
			result[key] = ""
		}
	}

	return result, nil

}

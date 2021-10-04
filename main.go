package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	kafka "github.com/segmentio/kafka-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type User struct {
	ID           int64
	Name         string
	WalletAmount int64
}

/*
{
	"id": 1,
	"name": jase,
	"walletAmount": 100
}
*/

func (User) TableName() string {
	return "users_jase"
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	db, err := initDB()
	if err != nil {
		log.Printf("initDB get error: %v\n", err)
		return
	}

	redisClient, err := initRedis()
	if err != nil {
		log.Printf("initRedis get error: %v\n", err)
		return
	}

	kafkaWriter := initKafka()

	go consumerCashTopic(db)

	http.HandleFunc("/cash", func(w http.ResponseWriter, r *http.Request) {
		m := r.URL.Query()
		names := m["name"]
		if len(names) != 1 {
			fmt.Fprintln(w, "name 輸入不合法")
			return
		}
		name := names[0]

		amountSlice := m["amount"]
		if len(amountSlice) != 1 {
			fmt.Fprintln(w, "找不到 amount key")
			return
		}

		amount, err := strconv.Atoi(amountSlice[0])
		if err != nil {
			fmt.Fprintln(w, err)
			return
		}

		// 搶 Redis 鎖
		lockSuccess := redisClient.SetNX(name, "lock"+"jase", 10*time.Second).Val()
		if lockSuccess { // 如果有搶到鎖
			defer func() {
				// 釋放鎖(先確認鎖還是不是自己的)
				lockVal := redisClient.Get(name).String()
				if lockVal == "lock"+"jase" {
					redisClient.Del(name)
				}
			}()
			// 取得資料庫的用戶錢包
			var user = &User{}
			err := db.Where("name = ?", name).First(&user).Error
			if err != nil {
				fmt.Fprintln(w, "db.Where get error: "+err.Error()+", name = "+name)
				return
			}
			// 增減用戶錢包
			user.WalletAmount += int64(amount)

			// 更新用戶錢包金額
			err = db.Model(user).Update("wallet_amount", user.WalletAmount).Error
			if err != nil {
				fmt.Fprintln(w, "db.Model(user).Update get error: "+err.Error()+", user: "+fmt.Sprintf("%+v", user))
				return
			}
			needRollback := false
			defer func() {
				if needRollback {
					// 回朔用戶錢包金額
					err = db.Model(user).Update("wallet_amount", user.WalletAmount-int64(amount)).Error
					if err != nil {
						fmt.Fprintln(w, "got err but db.Model(user).Update get error: "+err.Error()+", user: "+fmt.Sprintf("%+v", user))
						return
					}
				}
			}()

			// Kafka 消息傳遞給其他同學的服務
			b, err := json.Marshal(user)
			if err != nil {
				needRollback = true
				fmt.Fprintln(w, err)
				return
			}
			ctx := context.Background()
			err = kafkaWriter.WriteMessages(ctx, kafka.Message{Value: b})
			if err != nil {
				needRollback = true
				fmt.Fprintln(w, err)
				return
			}
		} else { // 沒搶到鎖
			fmt.Fprintln(w, "不好意思你沒搶到鎖")
			return
		}
		fmt.Fprintln(w, "成功增減錢")
	})

	http.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		m := r.URL.Query() // ../ping?name=jase&title=student&amount=100&... -> map[name] = []string{"jase"}, map[title] = []string{"stuent"}, map["amount"] = []string{"100"}
		names := m["name"]

		if len(names) != 0 {
			fmt.Fprintf(w, "Hello "+names[0]) // names := []string{"a", "b", "c"}, names[0] = "a", names[1] = "b" ...
			switch names[0] {
			case "天麒教授":
				fmt.Fprintln(w, "教授您好")
			default:
				fmt.Fprintln(w, "Hi")
			}

			var user = &User{}
			user.Name = names[0]
			err := db.Where("name = ?", user.Name).First(&user).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					err = db.Create(user).Error
					if err != nil {
						fmt.Fprintln(w, err)
						return
					}
				} else {
					fmt.Fprintln(w, err)
					return
				}
			} else {
				fmt.Fprintln(w, "您已註冊過了")
			}
		}
		fmt.Fprintln(w, "Hello World")
	})

	fmt.Println("server start...")
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func consumerCashTopic(db *gorm.DB) {
	topic := "nim7i0vg-cash"
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"dory-01.srvs.cloudkafka.com:9094", "dory-02.srvs.cloudkafka.com:9094", "dory-03.srvs.cloudkafka.com:9094"},
		Topic:    topic,
		MaxWait:  500 * time.Millisecond,
		MinBytes: 1,
		MaxBytes: 100,
	})
	ctx := context.Background()
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("reader.ReadMessage() get err : %v", err)
			continue
		}
		b := msg.Value
		fmt.Println(string(b))
		var user = &User{}
		err = json.Unmarshal(b, user)
		if err != nil {
			fmt.Printf("consumerCashTopic get err : %v", err)
			continue
		}
		if user.Name == "" {
			continue
		}
		user.ID = 0
		// 檢查用戶是否存在，如果不存在就創建
		var myUser = &User{
			Name:         user.Name,
			WalletAmount: user.WalletAmount,
		}
		err = db.Where("name = ?", user.Name).FirstOrCreate(myUser).Error
		if err != nil {
			fmt.Printf("FirstOrCreate get err: %v", err)
			continue
		}
		if myUser.WalletAmount != user.WalletAmount {
			// 更新用戶錢包金額
			err = db.Model(&User{}).Update("wallet_amount", user.WalletAmount).Where("name = ?", user.Name).Error
			if err != nil {
				fmt.Println("db.Model(user).Update get error: " + err.Error() + ", user: " + fmt.Sprintf("%+v", user))
				continue
			}
		}
	}
}

func initKafka() *kafka.Writer {
	topic := "nim7i0vg-cash"
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"dory-01.srvs.cloudkafka.com:9094", "dory-02.srvs.cloudkafka.com:9094", "dory-03.srvs.cloudkafka.com:9094"},
		Topic:   topic,
	})
}

func initRedis() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     "redis-15690.c257.us-east-1-3.ec2.cloud.redislabs.com:15690",
		Password: "T0PCm03FfnkVLSOIzCsDqwgCYHbq52Dk",
		DB:       0, // use default DB
	})
	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	return client, nil
}

func initDB() (*gorm.DB, error) {
	var dsn = "vsgzsg4ieoutdysr:zxh5755xi5804kfr@tcp(nnsgluut5mye50or.cbetxkdyhwsb.us-east-1.rds.amazonaws.com:3306)/rz4cip81n7mk2fdk?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return db, nil
}

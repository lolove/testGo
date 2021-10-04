package main

import (
	"fmt"
  kafka "github.com/segmentio/kafka-go"
	"github.com/go-redis/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
  "encoding/json"
  "context"
)

const (
	UserName     string = "vsgzsg4ieoutdysr"
	Password     string = "zxh5755xi5804kfr"
	Addr         string = "nnsgluut5mye50or.cbetxkdyhwsb.us-east-1.rds.amazonaws.com"
	Port         int    = 3306
	Database     string = "rz4cip81n7mk2fdk"
	MaxLifetime  int    = 10
	MaxOpenConns int    = 10
	MaxIdleConns int    = 10
)

type Users struct {
	ID           int64
	Name         string
	WalletAmount int64
}

func main() {
	// cnt := 3
	// Heroku 的 PORT 是放在「環境變數」中，所以用 os.Gatenv("PORT") 載入

	port := os.Getenv("PORT")
	if port == "" {
		// log.Fatal("$PORT must be set")
		port = "8080"
	}

  kafkaConn,err := initKafka() 
	// 123
	db, err := initDB()
	if err != nil {
		log.Print(err)
		return
	}
	redisClient, err := initRedis()
	if err != nil {
		log.Print(err)
		return
	}

	http.HandleFunc("/cash", func(w http.ResponseWriter, r *http.Request) {
		m := r.URL.Query()
		names := m["name"]
		amountSlice := m["amount"]

		if len(names) != 1 {
			fmt.Fprintln(w, "name 輸入不合法")
			return
		}

		name :=names[0]

		if len(amountSlice) != 1 {
			fmt.Fprintln(w, "找不到 amount key")
			return
		}

		amount, err := strconv.Atoi(amountSlice[0])
		if err != nil {
			fmt.Fprintln(w, err)
			return
		}

		lockSuccess:=redisClient.SetNX(name,"lock"+"mingyu",10+time.Second).Val()
		if lockSuccess{
			user := &Users{}
			err := db.Where("name = ?", name).First(&user).Error

			if err !=nil{
				fmt.Fprintln(w, "user is not exixt")
				return
			}
			user.WalletAmount+=int64(amount)
			if user.WalletAmount<0{
				fmt.Fprintln(w, name+"是窮鬼，他的wallet沒錢了，所以交易失敗!!")
				return
			}

			err =db.Model(user).Update("wallet_amount",user.WalletAmount).Error
			if err !=nil{
				fmt.Fprintln(w,err)
				return
			}

      b,err:= json.Marshal(user)

      _,err = kafkaConn.Write(b)

			fmt.Fprintln(w,(name+"的wallet還有"+strconv.Itoa(int(user.WalletAmount))))

			lockVal :=redisClient.Get(name).String()
			if lockVal=="lock"+"mingyu"{
				redisClient.Del(name)
			}
		}else{
			fmt.Fprintln(w, "not get lock")
			return
		}

	})

	http.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		// Get Query Val
		m := r.URL.Query()

		names := m["name"]

		// if len(names) != 0 {
		// 	switch names[0] {
		// 	case "學長":
		// 		fmt.Fprintf(w, "Hello , 學長")
		// 		break
		// 	case "學姊":
		// 		fmt.Fprintf(w, "Hello , 學姊")
		// 		break
		// 	default:
		// 		fmt.Fprintf(w, "我好帥")
		// 	}
		// 	// return
		// }

		user := &Users{}
		user.Name = names[0]
		err := db.Where("name = ?", user.Name).First(&user).Error

		if err != nil {
			// user.ID = cnt
			err = db.Create(user).Error
			if err != nil {
				fmt.Fprintln(w, "add user NG")

			} else {
				fmt.Fprintln(w, "add user OK")
				// cnt++
			}

		} else {
			fmt.Fprintln(w, "the same user is exixt in DB")
		}
		// fmt.Println(err)

		fmt.Print("Hello")
		fmt.Fprintf(w, "Hello World")

	})

	fmt.Println("Server Start ... ")
	// log.Fatal(http.ListenAndServe(":8080", nil))
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func initDB() (*gorm.DB, error) {
	addr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True", UserName, Password, Addr, Port, Database)
	db, err := gorm.Open(mysql.Open(addr), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return db, nil
}

func initRedis() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     "redis-15690.c257.us-east-1-3.ec2.cloud.redislabs.com:15690",
		Password: "T0PCm03FfnkVLSOIzCsDqwgCYHbq52Dk", // no password set
		DB:       0,                                  // use default DB
	})
	pong, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	log.Print(pong)
	return client, nil
}


func initKafka() (*kafka.Conn ,error){
	topic := "nim7i0vg-cash"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "dory-01.srvs.cloudkafka.com:9094,dory-02.srvs.cloudkafka.com:9094,dory-03.srvs.cloudkafka.com:9094", topic, partition)
	return conn, err
}

func consumerCashTopic(conn *kafka.Conn,db *gorm.DB){
  batch := conn.ReadBatch(10e3, 1e6)
  b:= make([]byte,10e3)
  for{
    _,err := batch.Read(b)
    if err != nil{
      break
    }

    fmt.Println(string((b)))
    var user  = &Users{}
    err = json.Unmarshal(b, user)
    if err != nil{
      fmt.Printf("consumerCashTopic get err : %v",err)
      continue
    }

    if user.Name == ""{
      continue
    }

    user.ID = 0

    var myUser = &Users{
      Name:user.Name,
      WalletAmount:user.WalletAmount,
    }

    err = db.Where("name = ?", user.Name).FirstOrCreate(myUser).Error

    if myUser.WalletAmount != user.WalletAmount {
      err = db.Model(&Users{}).Update("wallet_amount", user.WalletAmount).Where("name = ?", user.Name).Error
    }
    
  }
}

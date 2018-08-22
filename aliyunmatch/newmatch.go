package aliyunmatch

import (
	"os"
	"fmt"
	"bufio"
	"io"
	"bytes"
	"strings"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"database/sql"
	"time"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"strconv"
	"runtime"
	"github.com/syndtr/goleveldb/leveldb"
	"crypto/md5"
	"encoding/hex"
	"crypto/rand"
	"encoding/base64"
)

var Db *sqlx.DB
var Gdb *gorm.DB
var LevelDb *leveldb.DB

func init() {
	database, err := sqlx.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/match?charset=utf8")
	if err != nil {
		fmt.Println("open mysql failed,", err)
		return
	}
	Db = database

	gormDb, err := gorm.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/match?charset=utf8")
	if err != nil {
		fmt.Println("open gorm mysql failed,", err)
		return
	}
	Gdb = gormDb

	lDb, err := leveldb.OpenFile("/home/liuj-ai/LevelDB/alimatch/matchdata", nil)
	if err != nil {
		fmt.Println("open level db failed,", err)
		return
	}
	LevelDb = lDb

}

func CloseDb() {
	defer Db.Close()
	defer Gdb.Close()
	defer LevelDb.Close()
}

func checkErr(errMasg error) {
	if errMasg != nil {
		panic(errMasg)
	}
}

//================================================================ 加载比赛数据 ================================================================
func ReadDataToMysql(itempath string, userpath string) int {
	countItem := 0
	conn, connErr := Db.Begin()
	if connErr != nil {
		return countItem
	}

	var fileItem, itemErr = os.Open(itempath)
	if itemErr != nil {
		fmt.Errorf("Error create file:%s", itemErr.Error())
	}
	defer fileItem.Close()
	readerItem := bufio.NewReader(fileItem)

	var userFile, userErr = os.Open(userpath)
	if userErr != nil {
		fmt.Errorf("Error create file:%s", userErr.Error())
	}
	defer userFile.Close()
	readerUser := bufio.NewReader(userFile)

	var buf []byte = nil
	_ = readerItem
loopItem:
	for {
		countItem++
		line, isPrefix, err := readerItem.ReadLine()
		if err != nil {
			if err == io.EOF {
				fmt.Println("File read ok!")
				break loopItem
			} else {
				fmt.Println("Read file error!", err)
			}
		}
		if buf == nil && !isPrefix {
			buf = line
			LineItemToMysql(string(buf), conn, countItem)
			buf = nil
		} else {
			if !isPrefix {
				mergeBytes := make([][]byte, 2)
				mergeBytes[0] = buf
				mergeBytes[1] = line
				buf = bytes.Join(mergeBytes, []byte(""))
				LineItemToMysql(string(buf), conn, countItem)
				buf = nil
			} else {
				buf = BytesCombine(buf, line)
			}
		}
	}
	buf = nil
	countUser := 0
loopUser:
	for {
		countUser++
		line, isPrefix, err := readerUser.ReadLine()
		if err != nil {
			if err == io.EOF {
				fmt.Println("File read ok!")
				break loopUser
			} else {
				fmt.Println("Read file error!", err)
			}
		}
		if buf == nil && !isPrefix {
			buf = line
			LineUserToMysql(string(buf), conn, countUser)
			buf = nil
		} else {
			if !isPrefix {
				mergeBytes := make([][]byte, 2)
				mergeBytes[0] = buf
				mergeBytes[1] = line
				buf = bytes.Join(mergeBytes, []byte(""))
				LineUserToMysql(string(buf), conn, countUser)
				buf = nil
			} else {
				buf = BytesCombine(buf, line)
			}
		}
	}
	conn.Commit()
	return countItem + countUser
}

func LineItemToMysql(line string, tx *sql.Tx, count int) {
	//fmt.Println(line)
	if count > 1 {
		buf := bytes.NewBufferString("insert into tianchi_fresh_comp_train_item values(")
		dataArr := strings.Split(line, ",")
		buf.WriteString(fmt.Sprint(dataArr[0]))
		buf.WriteString(",")
		buf.WriteString("'")
		buf.WriteString(fmt.Sprint(dataArr[1]))
		buf.WriteString("'")
		buf.WriteString(",")
		buf.WriteString(fmt.Sprint(dataArr[2]))
		buf.WriteString(")")
		sql := buf.String()
		fmt.Println(sql)
		row, error := tx.Exec(sql)
		if error != nil {
			fmt.Println("exec failed,", error)
			panic(error)
		}
		_ = row
	}
}

func LineUserToMysql(line string, tx *sql.Tx, count int) {
	//fmt.Println(line)
	if count > 1 {
		buf := bytes.NewBufferString("insert into tianchi_fresh_comp_train_user values(")
		dataArr := strings.Split(line, ",")
		buf.WriteString(fmt.Sprint(dataArr[0]))
		buf.WriteString(",")
		buf.WriteString(fmt.Sprint(dataArr[1]))
		buf.WriteString(",")
		buf.WriteString(fmt.Sprint(dataArr[2]))
		buf.WriteString(",")
		buf.WriteString("'")
		buf.WriteString(fmt.Sprint(dataArr[3]))
		buf.WriteString("'")
		buf.WriteString(",")
		buf.WriteString(fmt.Sprint(dataArr[4]))
		buf.WriteString(",")
		buf.WriteString("str_to_date(")
		buf.WriteString("'")
		buf.WriteString(fmt.Sprint(dataArr[5]))
		buf.WriteString("'")
		buf.WriteString(",")
		buf.WriteString("'")
		buf.WriteString("%Y-%m-%d %H")
		buf.WriteString("'")
		buf.WriteString(")")
		buf.WriteString(")")
		sql := buf.String()
		fmt.Println(sql)
		row, error := tx.Exec(sql)
		if error != nil {
			fmt.Println("exec failed,", error)
			panic(error)
		}
		_ = row
	}
}

func BytesCombine(pBytes ...[]byte) []byte {
	return bytes.Join(pBytes, []byte(""))
}

//================================================================ Step 1 ================================================================

var userIdNumbers []int64

type MatchUser struct {
	UserId       int64     `gorm:"column:user_id"`
	ItemId       int64     `gorm:"column:item_id"`
	BehaviorType int       `gorm:"column:behavior_type"`
	UserGeoHash  string    `gorm:"column:user_geohash"`
	Time         time.Time `gorm:"column:time"`
}

func (MatchUser) TableName() string {
	return "tianchi_fresh_comp_train_user"
}

type StepOneOutKV struct {
	Key   string `gorm:"column:key"`
	Value string `gorm:"column:value"`
}

func (StepOneOutKV) TableName() string {
	return "tianchi_fresh_comp_train_kv_step_one"
}

type Result struct {
	UserId       int64 `gorm:"column:user_id"`
	ItemId       int64 `gorm:"column:item_id"`
	BehaviorType int64 `gorm:"column:behavior_type"`
}

func StepOne() {
	Tx := Gdb.Begin()
	defer Tx.Commit()

	var userIds []Result
	Tx.Raw("SELECT user_id FROM tianchi_fresh_comp_train_user GROUP BY user_id").Scan(&userIds)
	inToDbCount := 0
	if len(userIds) > 0 {
		for _, uId := range userIds {
			var result []Result
			Tx.Raw("SELECT user_id ,item_id, behavior_type FROM tianchi_fresh_comp_train_user WHERE user_id = ? GROUP BY user_id,item_id,behavior_type", uId.UserId).Scan(&result)
			userIdNumbers = append(userIdNumbers, uId.UserId)
			if len(result) > 0 {
				var currentKey string
				var currentValue string
				var itemBehaviorTypeMap = make(map[string]string)
				for _, rs := range result {
					userId := strconv.FormatInt(rs.UserId, 10)
					itemId := strconv.FormatInt(rs.ItemId, 10)
					behaviorType := strconv.FormatInt(rs.BehaviorType, 10)
					if len(currentKey) == 0 {
						currentKey = userId
					}
					data, exist := itemBehaviorTypeMap[itemId]
					if exist {
						if behaviorType > data {
							itemBehaviorTypeMap[itemId] = behaviorType
						}
					} else {
						itemBehaviorTypeMap[itemId] = behaviorType
					}
				}
				isFirst := true
				if len(itemBehaviorTypeMap) > 0 {
					buf := bytes.NewBufferString("")
					for k, v := range itemBehaviorTypeMap {
						if !isFirst {
							buf.WriteString(",")
						} else {
							isFirst = false
						}
						buf.WriteString(k)
						buf.WriteString(":")
						buf.WriteString(v)
					}
					currentValue = buf.String()

					if len(currentKey) > 0 && len(currentValue) > 0 {
						err := LevelDb.Put([]byte(currentKey), []byte(currentValue), nil)
						if err != nil {
							panic(err)
						} else {
							inToDbCount++
							fmt.Printf("入库成功userId=%s  |  value=%s \n", currentKey, currentValue)
						}
						/* leveldb代替mysql
						err := Tx.Create(StepOneOutKV{Key: currentKey, Value: currentValue}).Error
						if err != nil {
							panic(err)
						} else {
							inToDbCount++
							fmt.Printf("入库成功userId=%s  |  value=%s \n", currentKey, currentValue)
						}
						*/
					}
				}
			}
		}
		fmt.Printf("step 1 入库 tianchi_fresh_comp_train_kv_step_one 总量 %d", inToDbCount)
	}
}

//================================================================ Step 2 ================================================================

type StepTwoPointOneOutKV struct {
	Key   string `gorm:"column:key"`
	Value int64  `gorm:"column:value"`
}

func (StepTwoPointOneOutKV) TableName() string {
	return "tianchi_fresh_comp_train_kv_step_two_point_one"
}

type StepTwoPointTwoOutKV struct {
	Key   string `gorm:"column:key"`
	Value int64  `gorm:"column:value"`
}

func (StepTwoPointTwoOutKV) TableName() string {
	return "tianchi_fresh_comp_train_kv_step_two_point_two"
}

func StepTwo() {
	//step 2-1
	if userIdNumbers == nil {
		Tx := Gdb.Begin()
		var userIds []Result
		Tx.Raw("SELECT user_id FROM tianchi_fresh_comp_train_user GROUP BY user_id").Scan(&userIds)
		userIdsLen := len(userIds)
		if userIdsLen > 0 {
			for _, uId := range userIds {
				userIdNumbers = append(userIdNumbers, uId.UserId)
			}
		}
		Tx.Commit()
	}

	totalUserNumLen := len(userIdNumbers)
	if totalUserNumLen > 0 {
		txCount := 50
		semaphore := make(chan int, txCount)
		stepTwoPointOneInToDbTotal := 0
		eachTxUserNum := totalUserNumLen / txCount
		userInc := 0
		var userGoExecList []int64
		userGoExecCount := 0
		for _, uId := range userIdNumbers {
			userInc++
			userGoExecList = append(userGoExecList, uId)
			if userInc%eachTxUserNum == 0 {
				go func(userGoExecList []int64, semaphore chan<- int) {
					buffCount := 0
					stepTwoPointOneInToDbCount := 0
					//goTx := Gdb.Begin()
					for _, uId := range userGoExecList {
						key := strconv.FormatInt(uId, 10)
						var result *StepOneOutKV
						exist, error := LevelDb.Has([]byte(key), nil)
						if error != nil {
							panic(error)
						} else {
							if exist {
								value, err := LevelDb.Get([]byte(key), nil)
								if err != nil {
									panic(error)
								} else {
									result = &StepOneOutKV{Key: key, Value: string(value)}
								}
							}
						}
						if result != nil && len(result.Value) > 0 && len(result.Key) > 0 {
							itemIds := result.Value
							itemArr := strings.Split(itemIds, ",")
							len := len(itemArr)
							for i := 0; i < len; i++ {
								item := itemArr[i]
								for j := 0; j < len; j++ {
									other := itemArr[j]
									assemble := strings.Split(item, ":")[0] + ":" + strings.Split(other, ":")[0]
									exist, _ := LevelDb.Has([]byte(assemble), nil)
									if exist {
									}
									buffCount++
								}
								/*
								insertSql := buff.String()
								fmt.Printf("step 2-1 入库 tianchi_fresh_comp_train_kv_step_two_point_one 正在执行中 sql = %s \n", insertSql)
								err := goTx.Exec(insertSql).Error
								//err := goTx.Create(StepTwoPointOneOutKV{Key: assemble, Value: 1}).Error
								if err != nil {
									panic(err)
								} else {
									stepTwoPointOneInToDbCount += buffCount
								}
								buffCount = 0
								buff.Reset()
								buff.WriteString("insert into `tianchi_fresh_comp_train_kv_step_two_point_one` (`key`, `value`) values ")
								*/
							}
						}
					}
					fmt.Printf("===============================  step 2-1 入库 tianchi_fresh_comp_train_kv_step_two_point_one 增量 %d  ===============================\n", stepTwoPointOneInToDbCount)
					//goTx.Commit()
					semaphore <- stepTwoPointOneInToDbCount
					runtime.GC()
				}(userGoExecList, semaphore)
				userGoExecCount++
				//userInc = 0
				userGoExecList = nil
			}
		}
		//var itemCountMap map[string]int64
		completeCount := 0
	loop:
		for {
			v, _ := <-semaphore
			completeCount++
			stepTwoPointOneInToDbTotal += v
			fmt.Printf("step 2-1 入库 tianchi_fresh_comp_train_kv_step_two_point_one 总量 %d  \n", stepTwoPointOneInToDbTotal)
			runtime.GC()
			if completeCount >= userGoExecCount {
				break loop
			}
		}

		fmt.Println("step 2-1 入库 tianchi_fresh_comp_train_kv_step_two_point_one 完成", )

		//step 2-2
		Tx := Gdb.Begin()
		Tx.Raw("INSERT INTO tianchi_fresh_comp_train_kv_step_two_point_two (`key`, `value`) SELECT `key` , SUM(`value`) FROM  tianchi_fresh_comp_train_kv_step_two_point_one GROUP BY `key`")
		Tx.Commit()
		fmt.Println("step 2-2 入库 tianchi_fresh_comp_train_kv_step_two_point_two 完成", )
	}
}

//生成32位md5字串
func GetMd5String(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

//生成Guid字串
func UniqueId() string {
	b := make([]byte, 48)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	return GetMd5String(base64.URLEncoding.EncodeToString(b))
}

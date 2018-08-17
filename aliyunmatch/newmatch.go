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
)

var Db *sqlx.DB
var Gdb *gorm.DB

func init() {
	database, err := sqlx.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/match?charset=utf8")
	if err != nil {
		fmt.Println("open mysql failed,", err)
		return
	}
	Db = database

	gormDb, err := gorm.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/match?charset=utf8")
	Gdb = gormDb

	if err != nil {
		fmt.Println("open gorm mysql failed,", err)
		return
	}
}

func CloseDb() {
	defer Db.Close()
	defer Gdb.Close()
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
						err := Tx.Create(StepOneOutKV{Key: currentKey, Value: currentValue}).Error
						if err != nil {
							panic(err)
						} else {
							inToDbCount++
							fmt.Printf("入库成功userId=%s  |  value=%s \n", currentKey, currentValue)
						}
					}
				}
			}
		}
		fmt.Printf("step 1 入库 tianchi_fresh_comp_train_kv_step_one 总量 %d", inToDbCount)
	}
}

//================================================================ Step 2 ================================================================

type StepTwoOutKV struct {
	Key   string `gorm:"column:key"`
	Value int64  `gorm:"column:value"`
}

func (StepTwoOutKV) TableName() string {
	return "tianchi_fresh_comp_train_kv_step_two"
}

func StepTwo() {
	Tx := Gdb.Begin()
	defer Tx.Commit()
	if userIdNumbers == nil {
		var userIds []Result
		Tx.Raw("SELECT user_id FROM tianchi_fresh_comp_train_user GROUP BY user_id").Scan(&userIds)
		if len(userIds) > 0 {
			userIdNumbers = make([]int64, len(userIds))
			for _, uId := range userIds {
				userIdNumbers = append(userIdNumbers, uId.UserId)
			}
		}
	}
	if len(userIdNumbers) > 0 {
		//var itemCountMap map[string]int64
		for _, uId := range userIdNumbers {
			var result []StepOneOutKV
			Tx.Raw("select * from tianchi_fresh_comp_train_kv_step_one where 'key' = ?", uId).Scan(&result)
			if len(result) > 0 {
				for _, rs := range result {
					itemIds := rs.Value
					itemArr := strings.Split(itemIds, ",")
					len := len(itemArr)
					for i := 0; i < len; i++ {
						item := itemArr[i]
						for j := 0; j < len; j++ {
							other := itemArr[j]
							assemble := item + ":" + other
							err := Tx.Create(StepTwoOutKV{Key: assemble, Value: 1}).Error
							if err != nil {
								panic(err)
							}
						}
					}
				}
			}
		}

	}
}

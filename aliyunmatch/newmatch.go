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

const LevelDBPath = "C:\\Users\\liuj-ai\\LeveDB\\match\\"

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
}

func CloseDb() {
	defer Db.Close()
	defer Gdb.Close()
	defer StepOneDataDb.Close()
	defer StepTwoPointOneDataDb.Close()
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

var StepOneDataDb *leveldb.DB

func StepOne() {
	fmt.Println("===============================  step 1 计算入库开始 -> levedb  ===============================")

	Tx := Gdb.Begin()
	defer Tx.Commit()

	var userIds []Result
	Tx.Raw("SELECT user_id FROM tianchi_fresh_comp_train_user GROUP BY user_id").Scan(&userIds)
	inToDbCount := 0

	fileName := LevelDBPath + "StepOneData"
	var err error
	StepOneDataDb, err = leveldb.OpenFile(fileName, nil)
	if err != nil {
		fmt.Printf("open level db failed step 1 fileName = {%s}err = {%s} \n", fileName, err)
		return
	}

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
						err := StepOneDataDb.Put([]byte(currentKey), []byte(currentValue), nil)
						if err != nil {
							panic(err)
						} else {
							inToDbCount++
							fmt.Printf("入库成功 key = {%s}  |  value = {%s} \n", currentKey, currentValue)
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

		fmt.Printf("===============================  step 1 计算入库完成 -> levedb 总量 %d  ===============================\n", inToDbCount)
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

var StepTwoPointOneDataDb *leveldb.DB

func assemble(semaphore chan<- int, uId int64, index int) {
	uniqueId := UniqueId()
	stepTwoPointOneInToDbCount := 0
	key := strconv.FormatInt(uId, 10)

	//读取StepOne结果集
	var result *StepOneOutKV
	exist, error := StepOneDataDb.Has([]byte(key), nil)
	if error != nil {
		panic(error)
	} else {
		if exist {
			value, err := StepOneDataDb.Get([]byte(key), nil)
			if err != nil {
				panic(error)
			} else {
				result = &StepOneOutKV{Key: key, Value: string(value)}
			}
		}
	}
	//商品Item组合入库
	if result != nil && len(result.Value) > 0 && len(result.Key) > 0 {
		itemIds := result.Value
		itemArr := strings.Split(itemIds, ",")
		len := len(itemArr)
		for i := 0; i < len; i++ {
			item := itemArr[i]
			for j := 0; j < len; j++ {
				other := itemArr[j]
				assemble := strings.Split(item, ":")[0] + ":" + strings.Split(other, ":")[0]
				exist, _ := StepTwoPointOneDataDb.Has([]byte(assemble), nil)
				if exist {
					v, _ := StepTwoPointOneDataDb.Get([]byte(assemble), nil)
					value, _ := strconv.Atoi(string(v))
					value++
					error := StepTwoPointOneDataDb.Put([]byte(assemble), []byte(strconv.Itoa(value)), nil)
					if error != nil {
						panic(error)
					}
				} else {
					StepTwoPointOneDataDb.Put([]byte(assemble), []byte(strconv.Itoa(1)), nil)
				}
				stepTwoPointOneInToDbCount++
				fmt.Printf("step 2-1 协程id = {%s} 正在执行入库 key = {%s}  已执行次数 count ={%d}  正在执行第{%d}位的用户{%s} \n", uniqueId, assemble, stepTwoPointOneInToDbCount, index, key)
			}
		}
	}
	fmt.Printf("===============================  step 2-1 入库 tianchi_fresh_comp_train_kv_step_two_point_one 增量 %d  ===============================\n", stepTwoPointOneInToDbCount)
	//goTx.Commit()
	semaphore <- stepTwoPointOneInToDbCount
	runtime.GC()
}

func StepTwo() {
	fmt.Println("===============================  step 2 计算入库开始 -> levedb  ===============================")
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

	//打开step 1 结果集
	if StepOneDataDb == nil {
		fileName := LevelDBPath + "StepOneData"
		var err error
		StepOneDataDb, err = leveldb.OpenFile(fileName, nil)
		if err != nil {
			fmt.Printf("open level db failed step 1 fileName = {%s}err = {%s} \n", fileName, err)
			return
		}
	}

	fileName := LevelDBPath + "StepTwoPointOneData"
	var err error
	StepTwoPointOneDataDb, err = leveldb.OpenFile(fileName, nil)
	if err != nil {
		fmt.Printf("open level db failed step 2-1 fileName = {%s}  err = {%s} \n", fileName, err)
		return
	}

	totalUserNumLen := len(userIdNumbers)
	if totalUserNumLen > 0 {
		semaphore := make(chan int, totalUserNumLen)
		stepTwoPointOneInToDbTotal := 0
		userGoExecCount := 0
		for index, uId := range userIdNumbers {
			go assemble(semaphore, uId, index)
			userGoExecCount++
			if userGoExecCount == 1 {
				for {
					v, _ := <-semaphore
					userGoExecCount--
					stepTwoPointOneInToDbTotal += v
					if userGoExecCount == 0 {
						fmt.Printf("step 2-1 入库增量 %d  \n", stepTwoPointOneInToDbTotal)
						runtime.GC()
						break
					}
				}
			}
		}
		fmt.Printf("step 2-1 入库总量 %d  \n", stepTwoPointOneInToDbTotal)
	}
	fmt.Println("===============================  step 2 计算入库完成 -> levedb  ===============================")
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

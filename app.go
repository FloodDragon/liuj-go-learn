package main

import (
	"fmt"
	"runtime"
	"liuj-go-learn/aliyunmatch"
)

// 定义一个 DivideError 结构
type DivideError struct {
	dividee int
	divider int
}

// 实现 `error` 接口
func (de *DivideError) Error() string {
	strFormat := `
    Cannot proceed, the divider is zero.
    dividee: %d
    divider: 0
`
	return fmt.Sprintf(strFormat, de.dividee)
}

func ProduceFinalizedGarbage() {
	x := &Garbage{}
	runtime.SetFinalizer(x, notify)
}

type Garbage struct{ a int }

func notify(f *Garbage) {
	stats := &runtime.MemStats{}
	runtime.ReadMemStats(stats)
	fmt.Printf("堆的使用信息  %d,%d,%d,%d \n", stats.HeapSys, stats.HeapAlloc,
		stats.HeapIdle, stats.HeapReleased)
	go ProduceFinalizedGarbage()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	go ProduceFinalizedGarbage()

	//操作mysql
	//mysql.TestMysql()

	//初始化阿里数据
	//count := aliyunmatch.ReadDataToMysql("C:\\Users\\liuj-ai\\Desktop\\阿里测试数据\\fresh_comp_offline\\tianchi_fresh_comp_train_item.csv", "C:\\Users\\liuj-ai\\Desktop\\阿里测试数据\\fresh_comp_offline\\tianchi_fresh_comp_train_user.csv")
	//fmt.Println(" last count = " + strconv.Itoa(count))

	//步骤一
	//aliyunmatch.StepOne()

	//步骤二
	aliyunmatch.StepTwo()
	aliyunmatch.CloseDb()

}

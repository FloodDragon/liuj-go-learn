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

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
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

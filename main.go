package main

import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
	"fmt"
	"encoding/json"
	"bufio"
	"os"
	"io/ioutil"
	"strings"

)
var (
	host=[]string{"172.16.1.16:2181"}
	timeout  = time.Second*5
	zk_root = "/"
	zk_module = "module"
	zk_sys = "sys"
	zk_sys_mq = "mq"
	zk_sys_redis = "redis"
	zk_sys_mysql = "mysql"
	item_list  =[]string{zk_sys_mq,zk_sys_redis,zk_sys_mysql,zk_module}
)

func ContainChildren(stat *zk.Stat) bool {
	return stat.NumChildren > 0
}

func ContainValue(stat *zk.Stat) bool {
	return stat.DataLength > 0
}

func main111(){
	con, _, err := zk.Connect(host, timeout)
	if err != nil{
		fmt.Println(err)
		return
	}
	defer con.Close()
	r:=StracePath(con,zk_root,zk_root)
	b,_:=json.Marshal(r)
	fmt.Println(string(b))
}

//便利函数
func StracePath(c *zk.Conn, path,suffix string)interface{}{
	tmp:=make(map[string]interface{})
	r,s,e:=c.Children(path)
	if e!=nil{
		fmt.Println(e,s)
		return nil
	}
	//叶子节点
	if !ContainChildren(s){
		data,_,_:=c.Get(path)
		//value:=make(map[string]interface{})
		//tmp[suffix]=string(data)
		return string(data)
	}
	//递归子节点
	for _,p := range r {
		if path == zk_root{
			tmp[p]=StracePath(c,path+p,p)
		} else {
			tmp[p]=StracePath(c,path+"/"+p,p)
		}

	}
	return tmp
}

func MakeChild()map[string]interface{}  {
	return make(map[string]interface{})

}

//读取管道输入
var (
	ZK_ADDR="-h"
	IMPORT="import"
	EXPORT="export"
	CLEAR="clear"
)

func Usage()  {
	fmt.Println("********************************************************")
	fmt.Println("\n Zookeeper Tools ")
	fmt.Println("\n ./zk_tools -option [args]")
	fmt.Println("\n ./option:[-h import export]")
	fmt.Println("example: \n ./zk_tools  -h 127.0.0.1:2181 export > 1.json")
	fmt.Println("example: \n ./zk_tools  -h 127.0.0.1:2181 import < 1.json")
	fmt.Println("\n  (-_^)  yifei.xu@jyblife.com")
	fmt.Println("*******************************************************")

}

type zkLogger struct{}

func (s*zkLogger) Printf(format string, a ...interface{}) {

}
var zlog = zkLogger{}
func main(){
	args:=os.Args
	if len(args) != 4{
		Usage()
		return
	}
	zk_host:=args[2]
	cmd:=args[3]
	con, _, err := zk.Connect([]string{zk_host}, timeout)
	con.SetLogger(&zlog)
	if err != nil{
		fmt.Println(err)
		return
	}
	defer con.Close()
	switch cmd {
	case EXPORT:
		r:=StracePath(con,zk_root,zk_root)
		b,_:=json.MarshalIndent(r,"\n","\t")
		fmt.Println(string(b))
	case IMPORT:
		r:=bufio.NewReader(os.Stdin)
		data,e:=ioutil.ReadAll(r);if e!=nil{
			fmt.Println("-----",e)
			return
		}

		fmt.Println("os.Args:",os.Args)
		fmt.Println("os.Argc:",len(os.Args))
		value := make(map[string]interface{})
		e=json.Unmarshal(data,&value);if e!=nil{
			fmt.Println(e)
		}
		//
		Update(con,"/",value)
		fmt.Println("import success!")
	case CLEAR:
		Delete(con,"/")
	default:
		Usage()
		return
	}
	return
}

func Update(conn *zk.Conn,path string,root interface{}){
	_,ok:= root.(map[string]interface{});if !ok {
		//fmt.Println("find path:",path,root)
		strRoot := root.(string)
		UpdateAndCreate(conn,path,[]byte(strRoot))
		return
	}else {
		for k, v := range root.(map[string]interface{}) {
			if path=="/" {
				Update(conn,"/"+k, v)
			}else {
				Update(conn,path+"/"+k, v)
			}
		}
	}
}

func UpdateAndCreate(conn *zk.Conn,path string, value []byte)  {
	b,_,_:=conn.Exists(path);
	if !b  {
			CreateDir(conn,path)
			_,e:=conn.Set(path,value,-1)
			fmt.Println("create path:",path,"value:",string(value))
			if e != nil {
				fmt.Println("errr, path",e,path)

		}
	}else
	{   //update
		dstv,_,_:=conn.Get(path)
		if string(dstv) == string(value){

			return
		}
		fmt.Println("update from:",path,"value",string(dstv),"to",string(value))
		_,e:=conn.Set(path,value,-1)
		if e != nil {
			fmt.Println(e)
		}
	}

}

func CreateDir(c *zk.Conn,path string)  {
	s:=strings.Split(path,"/")
	tmp:=""
	for _,item := range s {
		tmp=tmp+item

		b,_,_ :=c.Exists(tmp);if !b {
			c.Create(tmp,[]byte{},0,zk.WorldACL(zk.PermAll))
		}
		tmp+="/"
	}
}


func clear_main() {
	//flag.Parse()
	con, _, err := zk.Connect(host, timeout)
	if err != nil {
		fmt.Println(err)
		return
	}
	Delete(con,"/")

}
func Delete(c *zk.Conn,path string)  {

	d, s, _ := c.Children(path)
	fmt.Println(path)
	if s.NumChildren==0{
		fmt.Println("delete:",path)
		c.Delete(path,-1)
		return
	}
	for _, r := range d {
		if path =="/"{
			Delete(c, path+r)
		}else {
			Delete(c, path+"/"+r)
		}
	}

}
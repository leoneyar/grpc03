package main

import (
	"MapReduce/grpc03/pbfiles"
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/golang/snappy"
	"google.golang.org/grpc"
)

// const chunkSize = 1 << (19) //分块大小
// const fileNum = 32          //文件数量
// const serverNum = 2         //服务器数量
// const subPath = "D:/VSCode/gocode/src/MapReduce/book/dataset4/"

const chunkSize = 1 << (22) //分块大小
const fileNum = 24          //文件数量
const serverNum = 2         //服务器数量
const subPath = "D:/VSCode/gocode/src/MapReduce/book/dataset2/"

func main() {
	startTime := time.Now().UnixNano()
	//目的服务器地址
	targetStream := make([]pbfiles.EchoService_GetBidirectionalStreamingEchoClient, serverNum)
	targetCli := make([]*grpc.ClientConn, serverNum)
	serverState := [serverNum]chan struct{}{} //服务器的连接状态
	for i := 0; i < serverNum; i++ {
		serverState[i] = make(chan struct{})
		tempStr := "127.0.0.1:808" + strconv.Itoa(i)
		targetStream[i], targetCli[i] = clientStream(tempStr)
		defer targetCli[i].Close()
	}
	//结果集
	result := make([]map[string]int32, fileNum)
	for i := 0; i < fileNum; i++ { //分配内存
		result[i] = make(map[string]int32)
	}
	var lock sync.Mutex
	//接收服务端的数据
	for i := 0; i < serverNum; i++ {
		go func(i int) {
			for {
				in, err := targetStream[i].Recv()
				if err == io.EOF {
					close(serverState[i])
					return
				}
				if err != nil {
					panic(err)
				}
				receFileName := in.GetReceiveFileName()
				mergeResult(result[receFileName], in.GetRes(), &lock)
			}
		}(i)
	}

	//读取所有等待处理的文件路径
	path := make(chan string, fileNum)
	serverPool(path, targetStream)
	for i := 1; i <= fileNum; i++ {
		path <- subPath + strconv.Itoa(i) + ".txt"
	}
	close(path)

	//关闭与服务端连接
	for i := 0; i < serverNum; i++ {

		//targetStream[i].CloseSend()
		<-serverState[i]
	}
	endTime := time.Now().UnixNano()
	//写入txt
	for i := 0; i < fileNum; i++ {
		writeMapToFile(result[i], i+1)
	}
	finallyEndTime := time.Now().UnixNano()
	fmt.Println("通信及处理过程的时间:", float64((endTime-startTime))/1e9)
	fmt.Println("写入txt的时间:", float64((finallyEndTime-endTime))/1e9)
}

//创建与服务器的流连接
func clientStream(target string) (pbfiles.EchoService_GetBidirectionalStreamingEchoClient, *grpc.ClientConn) {
	cli, err := grpc.Dial(target, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	//defer cli.Close()
	c := pbfiles.NewEchoServiceClient(cli)
	stream, _ := c.GetBidirectionalStreamingEcho(context.Background())
	return stream, cli
}

//对每个文件进行分块发送
func fileBlocking(path string, stream pbfiles.EchoService_GetBidirectionalStreamingEchoClient, wg *sync.WaitGroup) *os.File {
	index := 0
	for i := 45; i < len(path); i++ {
		if path[i] >= '0' && path[i] <= '9' {
			continue
		} else {
			index = i
			break
		}
	}
	sendFileName, _ := strconv.Atoi(path[45:index]) //文件名
	fileInfo, err := os.Stat(path)
	if err != nil {
		panic(err)
	}
	chunkNum := math.Ceil(float64(fileInfo.Size()) / chunkSize)
	// 打开指定文件夹
	f, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	//defer f.Close()
	r := bufio.NewReader(f)
	for i := 0; i < int(chunkNum); i++ {
		buf := make([]byte, chunkSize)
		r.Read(buf)
		wg.Add(1)
		go func(b []byte) {
			defer wg.Done()
			bSnappy := snappy.Encode(nil, b)
			req := pbfiles.EchoRequest{Req: bSnappy, SendFileName: int32(sendFileName - 1)}
			err = stream.Send(&req)
			fmt.Println(sendFileName - 1)
			if err != nil {
				fmt.Println(err)
				return
			}
		}(buf)

	}
	// var i int64 = 1
	// for ; i <= int64(chunkNum); i++ {
	// 	wg.Add(1)
	// 	go func(i int64) {
	// 		defer wg.Done()
	// 		b := make([]byte, chunkSize)
	// 		f.Seek((i-1)*chunkSize, 0)
	// 		if len(b) > int(fileInfo.Size()-(i-1)*chunkSize) {
	// 			b = make([]byte, fileInfo.Size()-(i-1)*chunkSize)
	// 		}
	// 		f.Read(b)
	// 		bSnappy := snappy.Encode(nil, b)
	// 		req := pbfiles.EchoRequest{Req: bSnappy, SendFileName: int32(sendFileName - 1)}
	// 		err = stream.Send(&req)
	// 		if err != nil {
	// 			fmt.Println(err)
	// 			return
	// 		}
	// 	}(i)

	// }
	return f
}

//创建服务器池
func serverPool(fileChan chan string, targetStream []pbfiles.EchoService_GetBidirectionalStreamingEchoClient) {
	wg := make([]sync.WaitGroup, serverNum)
	for i := 0; i < serverNum; i++ {
		go func(i int) {
			tempF := []*os.File{}
			for path := range fileChan {
				tempF = append(tempF, fileBlocking(path, targetStream[i], &wg[i]))
			}
			wg[i].Wait()
			for _, f := range tempF {
				f.Close()
			}
			targetStream[i].CloseSend()
		}(i)
	}
}

//对接受的结果进行合并
func mergeResult(result map[string]int32, temp map[string]int32, lock *sync.Mutex) {
	lock.Lock()
	for k, v := range temp {
		result[k] += v
	}
	lock.Unlock()
}

//对结果map进行排序并写入txt
func writeMapToFile(result map[string]int32, num int) {
	resultFileName := "resultSet/result" + strconv.Itoa(num) + ".txt"
	resultFile, _ := os.Create(resultFileName)
	defer resultFile.Close()
	sortmap := []string{}

	for k := range result {
		sortmap = append(sortmap, k)
	}
	sort.Strings(sortmap) //将得到的结果进行排序
	for _, v := range sortmap {
		resultFile.WriteString(v + ":" + strconv.Itoa(int(result[v])) + "\n")
	}
}

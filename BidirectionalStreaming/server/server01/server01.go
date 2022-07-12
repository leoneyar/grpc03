package main

import (
	"MapReduce/grpc03/pbfiles"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/golang/snappy"
	"google.golang.org/grpc"
)

type echoService struct {
	pbfiles.UnimplementedEchoServiceServer
}

func (es *echoService) GetBidirectionalStreamingEcho(stream pbfiles.EchoService_GetBidirectionalStreamingEchoServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		strTemp := in.GetReq()
		str, _ := snappy.Decode(nil, strTemp)
		num := in.GetSendFileName()
		res := Map(string(str))
		err = stream.Send(&pbfiles.EchoResponse{Res: res, ReceiveFileName: num})
		fmt.Printf("taskNum: %v\n", num)
		if err != nil {
			return err
		}
	}
}

func Map(input string) (ans map[string]int32) {
	ans = make(map[string]int32)
	ss := strings.Fields(input)
	for _, v := range ss {
		word := strings.ToLower(v)
		for len(word) > 0 && (word[0] < 'a' || word[0] > 'z') {
			word = word[1:]
		}
		for len(word) > 0 && (word[len(word)-1] < 'a' || word[len(word)-1] > 'z') {
			word = word[:len(word)-1]
		}
		if word == "" {
			continue
		}
		ans[word]++
	}
	return
}
func main() {
	rpcs := grpc.NewServer()
	pbfiles.RegisterEchoServiceServer(rpcs, new(echoService))
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	defer lis.Close()
	rpcs.Serve(lis)
}

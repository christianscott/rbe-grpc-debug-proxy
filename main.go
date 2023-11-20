package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	lru "github.com/hashicorp/golang-lru/v2"
	"google.golang.org/genproto/googleapis/bytestream"
	bepb "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	var (
		target, clientCertFile, clientCertKeyFile, caFile, unix, port string
	)
	flag.StringVar(&target, "target", "", "proxy target")
	flag.StringVar(&caFile, "ca_cert", "", "ca file")
	flag.StringVar(&clientCertFile, "client_cert", "", "client cert file")
	flag.StringVar(&clientCertKeyFile, "client_cert_key", "", "client cert key file")
	flag.StringVar(&unix, "unix", "", "unix socket (optional)")
	flag.StringVar(&port, "port", "8080", "port (optional)")
	flag.Parse()

	if target == "" || caFile == "" || clientCertFile == "" || clientCertKeyFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	tlsConf := &tls.Config{}
	readCert, err := tls.LoadX509KeyPair(clientCertFile, clientCertKeyFile)
	if err != nil {
		panic(err)
	}
	tlsConf.Certificates = []tls.Certificate{readCert}

	caCert, err := os.ReadFile(caFile)
	if err != nil {
		panic(err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		panic("failed to append ca cert")
	}
	tlsConf.RootCAs = caCertPool

	creds := credentials.NewTLS(tlsConf)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}

	if unix != "" {
		dialer := func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", unix)
		}
		opts = append(opts, grpc.WithContextDialer(dialer))
	}

	conn, err := grpc.Dial(target, opts...)
	if err != nil {
		panic(err)
	}

	ac := repb.NewActionCacheClient(conn)
	caps := repb.NewCapabilitiesClient(conn)
	cas := repb.NewContentAddressableStorageClient(conn)
	exec := repb.NewExecutionClient(conn)
	bs := bytestream.NewByteStreamClient(conn)
	beps := bepb.NewPublishBuildEventClient(conn)

	fmt.Fprintf(os.Stderr, "checking connectivity to %s...", target)
	_, err = caps.GetCapabilities(context.Background(), &repb.GetCapabilitiesRequest{})
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(os.Stderr, " ok")

	cachingAC, err := newCachingActionClient(ac)
	if err != nil {
		panic(err)
	}

	srv := &server{
		ac:   cachingAC,
		caps: caps,
		cas:  cas,
		exec: exec,
		bs:   bs,
		beps: beps,
	}
	gsrv := grpc.NewServer()
	repb.RegisterActionCacheServer(gsrv, srv)
	repb.RegisterCapabilitiesServer(gsrv, srv)
	repb.RegisterContentAddressableStorageServer(gsrv, srv)
	repb.RegisterExecutionServer(gsrv, srv)
	bytestream.RegisterByteStreamServer(gsrv, srv)
	bepb.RegisterPublishBuildEventServer(gsrv, srv)

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(os.Stderr, "proxy server listening on grpc://localhost:"+port)
	if err := gsrv.Serve(listener); err != nil {
		panic(err)
	}
}

type cachingActionCacheClient struct {
	cache  *lru.Cache[string, *repb.ActionResult]
	client repb.ActionCacheClient
}

func newCachingActionClient(ac repb.ActionCacheClient) (*cachingActionCacheClient, error) {
	cache, err := lru.New[string, *repb.ActionResult](100_000)
	if err != nil {
		return nil, err
	}
	return &cachingActionCacheClient{
		cache:  cache,
		client: ac,
	}, nil
}

func (c *cachingActionCacheClient) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	key := fmt.Sprintf("%s:%s", req.InstanceName, req.ActionDigest.Hash)
	cached, ok := c.cache.Get(key)
	if ok {
		return cached, nil
	}
	res, err := c.client.GetActionResult(ctx, req)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	clone := proto.Clone(res).(*repb.ActionResult)
	c.cache.Add(key, clone)
	return res, nil
}

func (c *cachingActionCacheClient) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	key := fmt.Sprintf("%s:%s", req.InstanceName, req.ActionDigest.Hash)
	clone := proto.Clone(req.ActionResult).(*repb.ActionResult)
	c.cache.Add(key, clone)
	return c.client.UpdateActionResult(ctx, req)
}

type server struct {
	ac   *cachingActionCacheClient
	caps repb.CapabilitiesClient
	cas  repb.ContentAddressableStorageClient
	exec repb.ExecutionClient
	bs   bytestream.ByteStreamClient
	beps bepb.PublishBuildEventClient
}

type invocation struct {
	method    string
	req       any
	startedAt time.Time
	encoder   *json.Encoder
	id        int64
	rmd       *repb.RequestMetadata
}

var nextInvocationId atomic.Int64 = atomic.Int64{}

func invoke(method string, ctx context.Context, req any) *invocation {
	rmd := requestMetadataFromContext(ctx)
	inv := &invocation{method: method, req: req, rmd: rmd, id: nextInvocationId.Add(1), encoder: json.NewEncoder(io.Discard)}
	inv.start()
	return inv
}

func requestMetadataFromContext(ctx context.Context) *repb.RequestMetadata {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}

	values := md.Get("build.bazel.remote.execution.v2.requestmetadata-bin")
	if len(values) != 1 {
		return nil
	}

	rmd := &repb.RequestMetadata{}
	err := proto.Unmarshal([]byte(values[0]), rmd)
	if err != nil {
		return nil
	}

	return rmd
}

func (i *invocation) start() {
	i.startedAt = time.Now()
	i.encoder.Encode(map[string]interface{}{
		"method": i.method,
		"type":   "start",
		"req":    i.req,
		"id":     i.id,
	})
}

func (i *invocation) end() {
	i.encoder.Encode(map[string]interface{}{
		"method":  i.method,
		"type":    "end",
		"id":      i.id,
		"elapsed": time.Since(i.startedAt).Milliseconds(),
	})
}

// Capabilities
func (s *server) GetCapabilities(ctx context.Context, req *repb.GetCapabilitiesRequest) (*repb.ServerCapabilities, error) {
	i := invoke("GetCapabilities", ctx, req)
	defer i.end()
	return s.caps.GetCapabilities(ctx, req)
}

// ActionCache
func (s *server) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	i := invoke("GetActionResult", ctx, req)
	defer i.end()
	return s.ac.GetActionResult(ctx, req)
}
func (s *server) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	i := invoke("UpdateActionResult", ctx, req)
	defer i.end()
	return s.ac.UpdateActionResult(ctx, req)
}

// ContentAddressableStorage
func (s *server) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	i := invoke("FindMissingBlobs", ctx, req)
	defer i.end()
	return s.cas.FindMissingBlobs(ctx, req)
}
func (s *server) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	i := invoke("FindMissingBlobs", ctx, req)
	defer i.end()
	return s.cas.BatchUpdateBlobs(ctx, req)
}
func (s *server) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	i := invoke("FindMissingBlobs", ctx, req)
	defer i.end()
	return s.cas.BatchReadBlobs(ctx, req)
}
func (s *server) GetTree(in *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	i := invoke("FindMissingBlobs", nil)
	defer i.end()
	client, err := s.cas.GetTree(stream.Context(), in)
	if err != nil {
		return err
	}
	res, err := client.Recv()
	if err != nil {
		return err
	}
	err = stream.Send(res)
	if err != nil {
		return err
	}
	return nil
}

// Execution
func (s *server) Execute(in *repb.ExecuteRequest, stream repb.Execution_ExecuteServer) error {
	i := invoke("Execute", in)
	defer i.end()
	client, err := s.exec.Execute(stream.Context(), in)
	if err != nil {
		return err
	}
	for {
		op, err := client.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = stream.Send(op)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *server) WaitExecution(req *repb.WaitExecutionRequest, stream repb.Execution_WaitExecutionServer) error {
	i := invoke("WaitExecution", ctx, req)
	defer i.end()
	client, err := s.exec.WaitExecution(stream.Context(), req)
	if err != nil {
		return err
	}
	for {
		op, err := client.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = stream.Send(op)
		if err != nil {
			return err
		}
	}
	return nil
}

// ByteStream
func (s *server) Read(in *bytestream.ReadRequest, stream bytestream.ByteStream_ReadServer) error {
	i := invoke("Read", in)
	defer i.end()
	client, err := s.bs.Read(stream.Context(), in)
	if err != nil {
		return err
	}
	for {
		res, err := client.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = stream.Send(res)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *server) Write(stream bytestream.ByteStream_WriteServer) error {
	i := invoke("Write", nil)
	defer i.end()
	client, err := s.bs.Write(stream.Context())
	if err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("error receiving from stream: %v", err)
			return err
		}
		err = client.Send(req)
		if err != nil {
			log.Printf("error sending to client: %v", err)
			return err
		}
	}

	res, err := client.CloseAndRecv()
	if err != nil {
		return err
	}
	err = stream.SendAndClose(res)
	if err != nil {
		return err
	}
	return nil
}

func (s *server) QueryWriteStatus(ctx context.Context, req *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	i := invoke("QueryWriteStatus", ctx, req)
	defer i.end()
	return s.bs.QueryWriteStatus(ctx, req)
}

// BEP
func (s *server) PublishLifecycleEvent(ctx context.Context, req *bepb.PublishLifecycleEventRequest) (*emptypb.Empty, error) {
	i := invoke("PublishLifecycleEvent", ctx, req)
	defer i.end()
	return s.beps.PublishLifecycleEvent(ctx, req)
}

func (s *server) PublishBuildToolEventStream(stream bepb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	i := invoke("PublishBuildToolEventStream", nil)
	defer i.end()

	client, err := s.beps.PublishBuildToolEventStream(stream.Context())
	if err != nil {
		return err
	}

	var clientDone = make(chan error)
	go func() {
		for {
			res, err := client.Recv()
			if err == io.EOF {
				clientDone <- nil
				return
			}
			if err != nil {
				log.Printf("error receiving from client: %v", err)
				clientDone <- err
				return
			}
			err = stream.Send(res)
			if err != nil {
				log.Printf("error sending from client to server: %v", err)
				clientDone <- err
				return
			}
		}
	}()

	var serverDone = make(chan error)
	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				serverDone <- nil
				return
			}
			if err != nil {
				log.Printf("error receiving from stream: %v", err)
				serverDone <- err
				return
			}
			err = client.Send(res)
			if err != nil {
				log.Printf("error sending to client: %v", err)
				serverDone <- err
				return
			}
		}
	}()

	err = <-clientDone
	if err != nil {
		return err
	}
	err = <-serverDone
	if err != nil {
		return err
	}

	return nil
}

// func unimplemented(meth string) error {
// 	return status.Error(codes.Internal, fmt.Sprintf("unimplemented: %s", meth))
// }

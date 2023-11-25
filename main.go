package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

	srv, err := newServer(conn)
	if err != nil {
		panic(err)
	}

	fmt.Fprintf(os.Stderr, "checking connectivity to %s...", target)
	_, err = srv.GetCapabilities(context.Background(), &repb.GetCapabilitiesRequest{})
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(os.Stderr, " ok")
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

type invocation struct {
	method    string
	req       any
	startedAt time.Time
	logger    *log.Logger
	id        int64
	rmd       *repb.RequestMetadata
}

var nextInvocationId atomic.Int64 = atomic.Int64{}

func invoke(method string, ctx context.Context, req any) *invocation {
	rmd := requestMetadataFromContext(ctx)
	inv := &invocation{method: method, req: req, rmd: rmd, id: nextInvocationId.Add(1), logger: log.New(os.Stderr, "rbe-proxy ", log.LstdFlags)}
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
	invocationId := "<nil>"
	if i.rmd != nil {
		invocationId = i.rmd.ToolInvocationId
	}
	i.logger.Printf("%s %s", invocationId, i.method)
}

func (i *invocation) end() {
	// i.encoder.Encode(map[string]interface{}{
	// 	"method":  i.method,
	// 	"type":    "end",
	// 	"id":      i.id,
	// 	"elapsed": time.Since(i.startedAt).Milliseconds(),
	// })
}

type server struct {
	actionCache *lru.Cache[string, *repb.ActionResult]
	ac          repb.ActionCacheClient
	caps        repb.CapabilitiesClient
	digests     *lru.Cache[string, bool]
	cas         repb.ContentAddressableStorageClient
	exec        repb.ExecutionClient
	bs          bytestream.ByteStreamClient
	beps        bepb.PublishBuildEventClient
}

func newServer(conn *grpc.ClientConn) (*server, error) {
	actionCache, err := lru.New[string, *repb.ActionResult](100_000)
	if err != nil {
		return nil, err
	}
	digests, err := lru.New[string, bool](10_000_000)
	if err != nil {
		return nil, err
	}
	ac := repb.NewActionCacheClient(conn)
	caps := repb.NewCapabilitiesClient(conn)
	cas := repb.NewContentAddressableStorageClient(conn)
	exec := repb.NewExecutionClient(conn)
	bs := bytestream.NewByteStreamClient(conn)
	beps := bepb.NewPublishBuildEventClient(conn)
	return &server{
		actionCache: actionCache,
		digests:     digests,
		ac:          ac,
		caps:        caps,
		cas:         cas,
		exec:        exec,
		bs:          bs,
		beps:        beps,
	}, nil
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

	key := fmt.Sprintf("%s:%s", req.InstanceName, req.ActionDigest.Hash)
	cached, ok := s.actionCache.Get(key)
	if ok {
		return cached, nil
	}
	res, err := s.ac.GetActionResult(ctx, req)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	clone := proto.Clone(res).(*repb.ActionResult)
	s.actionCache.Add(key, clone)
	return res, nil
}

func (s *server) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	i := invoke("UpdateActionResult", ctx, req)
	defer i.end()

	key := fmt.Sprintf("%s:%s", req.InstanceName, req.ActionDigest.Hash)
	clone := proto.Clone(req.ActionResult).(*repb.ActionResult)
	s.actionCache.Add(key, clone)
	return s.ac.UpdateActionResult(ctx, req)
}

func keyForDigest(d *repb.Digest) string {
	return fmt.Sprintf("%s:%s", d.Hash, d.SizeBytes)
}

// ContentAddressableStorage
func (s *server) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	i := invoke("FindMissingBlobs", ctx, req)
	defer i.end()

	var missingSlice []*repb.Digest
	missingMap := make(map[string]*repb.Digest)
	// filter out any digests that are already in the cache
	for _, d := range req.BlobDigests {
		key := keyForDigest(d)
		_, ok := s.digests.Get(key)
		if !ok {
			missingSlice = append(missingSlice, d)
			missingMap[key] = d
		}
	}
	req.BlobDigests = missingSlice

	// ask the upstream cache about any digests that we don't know about
	res, err := s.cas.FindMissingBlobs(ctx, req)
	if err != nil {
		return nil, err
	}

	// delete the digests that are still missing from the upstream CAS
	for _, d := range res.MissingBlobDigests {
		delete(missingMap, keyForDigest(d))
	}
	// add the digests that are no longer missing to the cache
	for k, _ := range missingMap {
		s.digests.Add(k, true)
	}

	return res, nil
}
func (s *server) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	i := invoke("BatchUpdateBlobs", ctx, req)
	defer i.end()
	for _, req := range req.Requests {
		s.digests.Add(keyForDigest(req.Digest), true)
	}
	return s.cas.BatchUpdateBlobs(ctx, req)
}
func (s *server) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	i := invoke("BatchReadBlobs", ctx, req)
	defer i.end()
	return s.cas.BatchReadBlobs(ctx, req)
}
func (s *server) GetTree(in *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	ctx := stream.Context()
	i := invoke("GetTree", ctx, nil)
	defer i.end()
	client, err := s.cas.GetTree(ctx, in)
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
	ctx := stream.Context()
	i := invoke("Execute", ctx, in)
	defer i.end()
	client, err := s.exec.Execute(ctx, in)
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
	ctx := stream.Context()
	i := invoke("WaitExecution", ctx, req)
	defer i.end()
	client, err := s.exec.WaitExecution(ctx, req)
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
	ctx := stream.Context()
	i := invoke("Read", ctx, in)
	defer i.end()
	client, err := s.bs.Read(ctx, in)
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
	ctx := stream.Context()
	i := invoke("Write", ctx, nil)
	defer i.end()
	client, err := s.bs.Write(ctx)
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
	ctx := stream.Context()
	i := invoke("PublishBuildToolEventStream", ctx, nil)
	defer i.end()

	client, err := s.beps.PublishBuildToolEventStream(ctx)
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

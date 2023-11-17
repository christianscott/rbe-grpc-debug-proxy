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
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
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

	// TODO: if possible, it would be smarter to re-use the TLS config from the client rather than setting it up again here
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

	fmt.Fprintf(os.Stderr, "checking connectivity to %s...", target)
	_, err = caps.GetCapabilities(context.Background(), &repb.GetCapabilitiesRequest{})
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(os.Stderr, " ok")

	srv := &server{ac, caps, cas, exec, bs}
	gsrv := grpc.NewServer()
	repb.RegisterActionCacheServer(gsrv, srv)
	repb.RegisterCapabilitiesServer(gsrv, srv)
	repb.RegisterContentAddressableStorageServer(gsrv, srv)
	repb.RegisterExecutionServer(gsrv, srv)
	bytestream.RegisterByteStreamServer(gsrv, srv)

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(os.Stderr, "proxy server listening on grpc://localhost:"+port)
	if err := gsrv.Serve(listener); err != nil {
		panic(err)
	}
}

type server struct {
	ac   repb.ActionCacheClient
	caps repb.CapabilitiesClient
	cas  repb.ContentAddressableStorageClient
	exec repb.ExecutionClient
	bs   bytestream.ByteStreamClient
}

type invocation struct {
	method    string
	req       any
	startedAt time.Time
	encoder   *json.Encoder
	id        int64
}

var nextInvocationId atomic.Int64 = atomic.Int64{}

func invoke(method string, req any) *invocation {
	inv := &invocation{method: method, req: req, id: nextInvocationId.Add(1), encoder: json.NewEncoder(os.Stdout)}
	inv.start()
	return inv
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
	i := invoke("GetCapabilities", req)
	defer i.end()
	return s.caps.GetCapabilities(ctx, req)
}

// ActionCache
func (s *server) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	i := invoke("GetActionResult", req)
	defer i.end()
	return s.ac.GetActionResult(ctx, req)
}
func (s *server) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	i := invoke("UpdateActionResult", req)
	defer i.end()
	return s.ac.UpdateActionResult(ctx, req)
}

// ContentAddressableStorage
func (s *server) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	i := invoke("FindMissingBlobs", req)
	defer i.end()
	return s.cas.FindMissingBlobs(ctx, req)
}
func (s *server) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	i := invoke("FindMissingBlobs", req)
	defer i.end()
	return s.cas.BatchUpdateBlobs(ctx, req)
}
func (s *server) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	i := invoke("FindMissingBlobs", req)
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
	i := invoke("WaitExecution", req)
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
	i := invoke("QueryWriteStatus", req)
	defer i.end()
	return s.bs.QueryWriteStatus(ctx, req)
}

func unimplemented(meth string) error {
	return status.Error(codes.Internal, fmt.Sprintf("unimplemented: %s", meth))
}

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

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
	flag.StringVar(&unix, "unix", "", "unix socket")
	flag.StringVar(&port, "port", "8080", "port")
	flag.Parse()

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

	_, err = caps.GetCapabilities(context.Background(), &repb.GetCapabilitiesRequest{})
	if err != nil {
		panic(err)
	}

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

// Capabilities
func (s *server) GetCapabilities(ctx context.Context, req *repb.GetCapabilitiesRequest) (*repb.ServerCapabilities, error) {
	log.Println("Called GetCapabilities")
	return s.caps.GetCapabilities(ctx, req)
}

// ActionCache
func (s *server) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	log.Println("Called GetActionResult")
	return s.ac.GetActionResult(ctx, req)
}
func (s *server) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	log.Println("Called UpdateActionResult")
	return s.ac.UpdateActionResult(ctx, req)
}

// ContentAddressableStorage
func (s *server) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	log.Println("Called FindMissingBlobs")
	return s.cas.FindMissingBlobs(ctx, req)
}
func (s *server) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	log.Println("Called BatchUpdateBlobs")
	return s.cas.BatchUpdateBlobs(ctx, req)
}
func (s *server) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	log.Println("Called BatchReadBlobs")
	return s.cas.BatchReadBlobs(ctx, req)
}
func (s *server) GetTree(*repb.GetTreeRequest, repb.ContentAddressableStorage_GetTreeServer) error {
	log.Println("Called GetTree")
	return unimplemented("GetTree")
}

// Execution
func (s *server) Execute(*repb.ExecuteRequest, repb.Execution_ExecuteServer) error {
	log.Println("Called Execute")
	return unimplemented("Execute")
}
func (s *server) WaitExecution(*repb.WaitExecutionRequest, repb.Execution_WaitExecutionServer) error {
	log.Println("Called WaitExecution")
	return unimplemented("WaitExecution")
}

// ByteStream
func (s *server) Read(req *bytestream.ReadRequest, srv bytestream.ByteStream_ReadServer) error {
	log.Println("Called Read")
	return unimplemented("Read")
}
func (s *server) Write(srv bytestream.ByteStream_WriteServer) error {
	log.Println("Called Write")
	return unimplemented("Write")
}
func (s *server) QueryWriteStatus(context.Context, *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	log.Println("Called QueryWriteStatus")
	return nil, unimplemented("QueryWriteStatus")
}

func unimplemented(meth string) error {
	return status.Error(codes.Internal, fmt.Sprintf("unimplemented: %s", meth))
}

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
func (s *server) GetTree(in *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	log.Println("Called GetTree")
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
	log.Printf("Called Execute: %s\n", in.ActionDigest.String())
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
func (s *server) WaitExecution(*repb.WaitExecutionRequest, repb.Execution_WaitExecutionServer) error {
	log.Println("Called WaitExecution")
	return unimplemented("WaitExecution")
}

// ByteStream
func (s *server) Read(in *bytestream.ReadRequest, stream bytestream.ByteStream_ReadServer) error {
	log.Printf("Called Read: %s\n", in.ResourceName)
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
	log.Printf("Called Write\n")
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
	log.Println("Called QueryWriteStatus")
	return s.bs.QueryWriteStatus(ctx, req)
}

func unimplemented(meth string) error {
	return status.Error(codes.Internal, fmt.Sprintf("unimplemented: %s", meth))
}

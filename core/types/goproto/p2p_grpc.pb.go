// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package goproto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// P2PNetworkClient is the client API for P2PNetwork service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type P2PNetworkClient interface {
	// kernel(rpc server)
	// tripods --call-->  kernel
	RequestPeer(ctx context.Context, in *StreamRequest, opts ...grpc.CallOption) (*StreamResponse, error)
	// kernel(rpc client)
	// kernel --call--> tripods
	HandleRequest(ctx context.Context, in *Bytes, opts ...grpc.CallOption) (*StreamResponse, error)
	AddTopic(ctx context.Context, in *String, opts ...grpc.CallOption) (*emptypb.Empty, error)
	PubP2P(ctx context.Context, in *PubRequest, opts ...grpc.CallOption) (*Err, error)
	SubP2P(ctx context.Context, in *SubRequest, opts ...grpc.CallOption) (*SubResponse, error)
}

type p2PNetworkClient struct {
	cc grpc.ClientConnInterface
}

func NewP2PNetworkClient(cc grpc.ClientConnInterface) P2PNetworkClient {
	return &p2PNetworkClient{cc}
}

func (c *p2PNetworkClient) RequestPeer(ctx context.Context, in *StreamRequest, opts ...grpc.CallOption) (*StreamResponse, error) {
	out := new(StreamResponse)
	err := c.cc.Invoke(ctx, "/P2pNetwork/RequestPeer", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *p2PNetworkClient) HandleRequest(ctx context.Context, in *Bytes, opts ...grpc.CallOption) (*StreamResponse, error) {
	out := new(StreamResponse)
	err := c.cc.Invoke(ctx, "/P2pNetwork/HandleRequest", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *p2PNetworkClient) AddTopic(ctx context.Context, in *String, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/P2pNetwork/AddTopic", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *p2PNetworkClient) PubP2P(ctx context.Context, in *PubRequest, opts ...grpc.CallOption) (*Err, error) {
	out := new(Err)
	err := c.cc.Invoke(ctx, "/P2pNetwork/PubP2P", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *p2PNetworkClient) SubP2P(ctx context.Context, in *SubRequest, opts ...grpc.CallOption) (*SubResponse, error) {
	out := new(SubResponse)
	err := c.cc.Invoke(ctx, "/P2pNetwork/SubP2P", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// P2PNetworkServer is the server API for P2PNetwork service.
// All implementations should embed UnimplementedP2PNetworkServer
// for forward compatibility
type P2PNetworkServer interface {
	// kernel(rpc server)
	// tripods --call-->  kernel
	RequestPeer(context.Context, *StreamRequest) (*StreamResponse, error)
	// kernel(rpc client)
	// kernel --call--> tripods
	HandleRequest(context.Context, *Bytes) (*StreamResponse, error)
	AddTopic(context.Context, *String) (*emptypb.Empty, error)
	PubP2P(context.Context, *PubRequest) (*Err, error)
	SubP2P(context.Context, *SubRequest) (*SubResponse, error)
}

// UnimplementedP2PNetworkServer should be embedded to have forward compatible implementations.
type UnimplementedP2PNetworkServer struct {
}

func (UnimplementedP2PNetworkServer) RequestPeer(context.Context, *StreamRequest) (*StreamResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestPeer not implemented")
}
func (UnimplementedP2PNetworkServer) HandleRequest(context.Context, *Bytes) (*StreamResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method HandleRequest not implemented")
}
func (UnimplementedP2PNetworkServer) AddTopic(context.Context, *String) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddTopic not implemented")
}
func (UnimplementedP2PNetworkServer) PubP2P(context.Context, *PubRequest) (*Err, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PubP2P not implemented")
}
func (UnimplementedP2PNetworkServer) SubP2P(context.Context, *SubRequest) (*SubResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SubP2P not implemented")
}

// UnsafeP2PNetworkServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to P2PNetworkServer will
// result in compilation errors.
type UnsafeP2PNetworkServer interface {
	mustEmbedUnimplementedP2PNetworkServer()
}

func RegisterP2PNetworkServer(s grpc.ServiceRegistrar, srv P2PNetworkServer) {
	s.RegisterService(&P2PNetwork_ServiceDesc, srv)
}

func _P2PNetwork_RequestPeer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StreamRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2PNetworkServer).RequestPeer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/P2pNetwork/RequestPeer",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2PNetworkServer).RequestPeer(ctx, req.(*StreamRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _P2PNetwork_HandleRequest_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Bytes)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2PNetworkServer).HandleRequest(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/P2pNetwork/HandleRequest",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2PNetworkServer).HandleRequest(ctx, req.(*Bytes))
	}
	return interceptor(ctx, in, info, handler)
}

func _P2PNetwork_AddTopic_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(String)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2PNetworkServer).AddTopic(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/P2pNetwork/AddTopic",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2PNetworkServer).AddTopic(ctx, req.(*String))
	}
	return interceptor(ctx, in, info, handler)
}

func _P2PNetwork_PubP2P_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PubRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2PNetworkServer).PubP2P(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/P2pNetwork/PubP2P",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2PNetworkServer).PubP2P(ctx, req.(*PubRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _P2PNetwork_SubP2P_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SubRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2PNetworkServer).SubP2P(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/P2pNetwork/SubP2P",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2PNetworkServer).SubP2P(ctx, req.(*SubRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// P2PNetwork_ServiceDesc is the grpc.ServiceDesc for P2PNetwork service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var P2PNetwork_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "P2pNetwork",
	HandlerType: (*P2PNetworkServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RequestPeer",
			Handler:    _P2PNetwork_RequestPeer_Handler,
		},
		{
			MethodName: "HandleRequest",
			Handler:    _P2PNetwork_HandleRequest_Handler,
		},
		{
			MethodName: "AddTopic",
			Handler:    _P2PNetwork_AddTopic_Handler,
		},
		{
			MethodName: "PubP2P",
			Handler:    _P2PNetwork_PubP2P_Handler,
		},
		{
			MethodName: "SubP2P",
			Handler:    _P2PNetwork_SubP2P_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "p2p.proto",
}

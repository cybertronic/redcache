import re

# Fix pkg/rpc/client.go
with open('pkg/rpc/client.go', 'r') as f:
    content = f.read()

old = (
    '\tconn, err := grpc.DialContext(ctx, addr,\n'
    '\t\tgrpc.WithTransportCredentials(insecure.NewCredentials()),\n'
    '\t\tgrpc.WithBlock(),\n'
    '\t\tgrpc.WithKeepaliveParams(keepalive.ClientParameters{\n'
    '\t\t\tTime:                10 * time.Second,\n'
    '\t\t\tTimeout:             3 * time.Second,\n'
    '\t\t\tPermitWithoutStream: true,\n'
    '\t\t}),\n'
    '\t\tgrpc.WithDefaultCallOptions(\n'
    '\t\t\tgrpc.MaxCallRecvMsgSize(100*1024*1024), // 100MB\n'
    '\t\t\tgrpc.MaxCallSendMsgSize(100*1024*1024), // 100MB\n'
    '\t\t),\n'
    '\t)\n'
    '\tif err != nil {\n'
    '\t\tc.recordFailure(addr)\n'
    '\t\treturn nil, fmt.Errorf("failed to connect to %s: %w", addr, err)\n'
    '\t}'
)

new = (
    '\tconn, err := grpc.NewClient(addr,\n'
    '\t\tgrpc.WithTransportCredentials(insecure.NewCredentials()),\n'
    '\t\tgrpc.WithKeepaliveParams(keepalive.ClientParameters{\n'
    '\t\t\tTime:                10 * time.Second,\n'
    '\t\t\tTimeout:             3 * time.Second,\n'
    '\t\t\tPermitWithoutStream: true,\n'
    '\t\t}),\n'
    '\t\tgrpc.WithDefaultCallOptions(\n'
    '\t\t\tgrpc.MaxCallRecvMsgSize(100*1024*1024), // 100MB\n'
    '\t\t\tgrpc.MaxCallSendMsgSize(100*1024*1024), // 100MB\n'
    '\t\t),\n'
    '\t)\n'
    '\tif err != nil {\n'
    '\t\tc.recordFailure(addr)\n'
    '\t\treturn nil, fmt.Errorf("failed to create client for %s: %w", addr, err)\n'
    '\t}\n'
    '\t// NewClient connects lazily; eagerly establish the connection within the\n'
    '\t// caller-supplied timeout so we fail fast on unreachable peers.\n'
    '\tconn.Connect()\n'
    '\tif !conn.WaitForStateChange(ctx, conn.GetState()) {\n'
    '\t\tconn.Close()\n'
    '\t\tc.recordFailure(addr)\n'
    '\t\treturn nil, fmt.Errorf("failed to connect to %s: %w", addr, ctx.Err())\n'
    '\t}'
)

if old in content:
    content = content.replace(old, new)
    with open('pkg/rpc/client.go', 'w') as f:
        f.write(content)
    print("pkg/rpc/client.go: patched DialContext -> NewClient")
else:
    print("ERROR: pattern not found in pkg/rpc/client.go")
    # Show what we have around line 127
    lines = content.split('\n')
    for i, line in enumerate(lines[122:145], start=123):
        print(f"  {i}: {repr(line)}")

# Fix pkg/distributed/grpc_client.go
with open('pkg/distributed/grpc_client.go', 'r') as f:
    content2 = f.read()

old2 = (
    '\tconn, err := grpc.Dial(addr, opts...)\n'
    '\tif err != nil {\n'
    '\t\treturn nil, fmt.Errorf("failed to dial %s: %w", addr, err)\n'
    '\t}'
)

new2 = (
    '\tconn, err := grpc.NewClient(addr, opts...)\n'
    '\tif err != nil {\n'
    '\t\treturn nil, fmt.Errorf("failed to create client for %s: %w", addr, err)\n'
    '\t}'
)

if old2 in content2:
    content2 = content2.replace(old2, new2)
    with open('pkg/distributed/grpc_client.go', 'w') as f:
        f.write(content2)
    print("pkg/distributed/grpc_client.go: patched Dial -> NewClient")
else:
    print("ERROR: pattern not found in pkg/distributed/grpc_client.go")
    lines = content2.split('\n')
    for i, line in enumerate(lines[72:88], start=73):
        print(f"  {i}: {repr(line)}")
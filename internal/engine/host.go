package engine

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/easytier/easytier/easytier-go/internal/artifact"
	"github.com/easytier/easytier/easytier-go/internal/contextutil"
	"github.com/easytier/easytier/easytier-go/internal/coreabi"
	"github.com/easytier/easytier/easytier-go/internal/hostabi"
	"github.com/easytier/easytier/easytier-go/internal/reactor"
	"github.com/easytier/easytier/easytier-go/platform"
	"github.com/metacubex/wazero"
	"github.com/metacubex/wazero/api"
	"github.com/metacubex/wazero/experimental"
	"github.com/metacubex/wazero/imports/wasi_snapshot_preview1"
)

const defaultPacketQueueCapacity = 64
const maximumPacketIngressBatch = 32

type Options struct {
	Services            platform.Services
	PacketQueueCapacity int
	Management          reactor.ManagementHandler
}

// Host owns the wazero runtime and the compiled core. Every instance and the
// WebClient run in a guest environment of their own: a core module instance
// with its own linear memory, host ABI module and reactor. Guests therefore
// execute in parallel, an operation ID chosen by one guest cannot collide with
// another's, a trap ends only the guest it happened in, and closing a guest
// releases every host resource it opened.
type Host struct {
	ctx    context.Context
	cancel context.CancelFunc

	runtime  wazero.Runtime
	compiled wazero.CompiledModule
	options  Options

	nextGuest atomic.Uint64

	mu        sync.Mutex
	closed    bool
	instances map[*Instance]struct{}
	webClient *WebClient
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func NewHost(ctx context.Context, options Options) (_ *Host, err error) {
	if ctx == nil {
		return nil, fmt.Errorf("create EasyTier engine host with nil context")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if options.PacketQueueCapacity == 0 {
		options.PacketQueueCapacity = defaultPacketQueueCapacity
	}
	if options.PacketQueueCapacity < 0 {
		return nil, fmt.Errorf("packet queue capacity must be positive")
	}

	lifetime, cancel := context.WithCancel(contextutil.WithoutCancel(ctx))
	runtime := wazero.NewRuntime(lifetime)
	var compiled wazero.CompiledModule
	defer func() {
		if err == nil {
			return
		}
		cancel()
		if compiled != nil {
			_ = compiled.Close(contextutil.WithoutCancel(ctx))
		}
		_ = runtime.Close(contextutil.WithoutCancel(ctx))
	}()

	if _, err = wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
		return nil, fmt.Errorf("instantiate WASI preview1: %w", err)
	}
	compiled, err = runtime.CompileModule(ctx, artifact.Core())
	if err != nil {
		return nil, fmt.Errorf("compile embedded EasyTier core: %w", err)
	}
	// Compiling the core allocates ~100MB of throwaway compiler state on the
	// Go heap. The Go runtime does not return it to the OS on its own after
	// the initiating GC, so the process would retain the compilation peak as
	// resident memory for its entire lifetime. NewHost runs once at startup,
	// outside the packet path, so reclaim it explicitly here.
	debug.FreeOSMemory()

	host := &Host{
		ctx:       lifetime,
		cancel:    cancel,
		runtime:   runtime,
		compiled:  compiled,
		options:   options,
		instances: make(map[*Instance]struct{}),
		closeDone: make(chan struct{}),
	}
	return host, nil
}

// guestEnvironment is one isolated core module instance and the host side it
// talks to.
type guestEnvironment struct {
	reactor    *reactor.Reactor
	hostModule api.Module
	module     api.Module
}

func (host *Host) newGuestEnvironment(ctx context.Context) (_ *guestEnvironment, err error) {
	id := host.nextGuest.Add(1)
	environment := &guestEnvironment{
		reactor: reactor.New(host.ctx, reactor.Options{
			Services:   host.options.Services,
			Management: host.options.Management,
		}),
	}
	defer func() {
		if err != nil {
			environment.close()
		}
	}()
	adapter, err := hostabi.New(environment.reactor)
	if err != nil {
		return nil, err
	}
	environment.hostModule, err = adapter.Instantiate(ctx, host.runtime, fmt.Sprintf("%s_%d", hostabi.ImportModule, id))
	if err != nil {
		return nil, fmt.Errorf("instantiate EasyTier host ABI: %w", err)
	}
	hostModule := environment.hostModule
	linkCtx := experimental.WithImportResolver(ctx, func(name string) api.Module {
		if name == hostabi.ImportModule {
			return hostModule
		}
		return nil
	})
	environment.module, err = host.runtime.InstantiateModule(
		linkCtx,
		host.compiled,
		newModuleConfig().WithName(fmt.Sprintf("easytier_core_%d", id)),
	)
	if err != nil {
		return nil, fmt.Errorf("instantiate embedded EasyTier core: %w", err)
	}
	return environment, nil
}

// close releases the guest and every host resource it opened.
func (environment *guestEnvironment) close() error {
	cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var closeErrors []error
	if environment.module != nil {
		closeErrors = append(closeErrors, environment.module.Close(cleanupContext))
	}
	environment.reactor.Close()
	if environment.hostModule != nil {
		closeErrors = append(closeErrors, environment.hostModule.Close(cleanupContext))
	}
	return errors.Join(closeErrors...)
}

func newModuleConfig() wazero.ModuleConfig {
	return wazero.NewModuleConfig().
		WithRandSource(rand.Reader).
		WithSysWalltime().
		WithSysNanotime().
		WithSysNanosleep()
}

func (host *Host) CreateInstance(
	ctx context.Context,
	configTOML string,
) (*Instance, error) {
	if host == nil {
		return nil, fmt.Errorf("create instance with nil EasyTier engine host")
	}
	if ctx == nil {
		return nil, fmt.Errorf("create EasyTier instance with nil context")
	}
	envelope, err := encodeCreateEnvelope(configTOML, host.options.Services.Snapshot)
	if err != nil {
		return nil, err
	}

	host.mu.Lock()
	closed := host.closed
	host.mu.Unlock()
	if closed {
		return nil, fmt.Errorf("create instance with closed EasyTier engine host")
	}
	// The guest is created without host.mu: other guests keep running while
	// this one initializes.
	environment, err := host.newGuestEnvironment(ctx)
	if err != nil {
		return nil, err
	}
	instanceReactor := environment.reactor
	packetSink, err := instanceReactor.RegisterPacketSink(host.options.PacketQueueCapacity)
	if err != nil {
		_ = environment.close()
		return nil, fmt.Errorf("register EasyTier packet sink: %w", err)
	}
	events := make(chan Event, instanceEventQueueCapacity)
	journal := newEventJournal()
	eventSink, err := instanceReactor.RegisterEventSink(func(kind, message string) bool {
		journal.add(kind, message)
		select {
		case events <- Event{Kind: kind, Message: message}:
			return true
		default:
			return false
		}
	})
	if err != nil {
		close(events)
		_ = environment.close()
		return nil, fmt.Errorf("register EasyTier event sink: %w", err)
	}
	core, err := coreabi.New(environment.module)
	if err == nil {
		err = core.Create(ctx, envelope, packetSink, eventSink)
	}
	if err != nil {
		close(events)
		_ = environment.close()
		return nil, err
	}

	lifetime, cancel := context.WithCancel(host.ctx)
	instance := &Instance{
		host:              host,
		ctx:               lifetime,
		cancel:            cancel,
		core:              core,
		dataPlane:         core,
		rpc:               core,
		environment:       environment,
		reactor:           instanceReactor,
		packetSink:        packetSink,
		eventSink:         eventSink,
		events:            events,
		journal:           journal,
		commands:          make(chan command, maximumPacketIngressBatch),
		dataPlaneCommands: make(chan dataPlaneCommand),
		rpcCommands:       make(chan rpcCommand),
		pendingOperations: make(
			map[coreabi.OperationID]*pendingOperation,
		),
		pendingRPCs: make(
			map[coreabi.RPCOperationID]*pendingRPC,
		),
		completions:    instanceReactor.Completions(),
		closeRequested: make(chan struct{}),
		done:           make(chan struct{}),
		running:        make(chan struct{}),
		stopped:        make(chan struct{}),
	}
	instance.state.Store(int32(coreabi.StateCreated))
	host.mu.Lock()
	if host.closed {
		host.mu.Unlock()
		cancel()
		close(events)
		_ = core.Drop(contextutil.WithoutCancel(ctx))
		_ = environment.close()
		return nil, fmt.Errorf("create instance with closed EasyTier engine host")
	}
	host.instances[instance] = struct{}{}
	host.mu.Unlock()
	go instance.run()
	return instance, nil
}

func (host *Host) Close(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("close EasyTier engine host with nil context")
	}
	host.closeOnce.Do(func() {
		host.mu.Lock()
		host.closed = true
		webClient := host.webClient
		instances := make([]*Instance, 0, len(host.instances))
		for instance := range host.instances {
			instances = append(instances, instance)
		}
		host.mu.Unlock()
		go host.shutdown(webClient, instances)
	})
	select {
	case <-host.closeDone:
		host.mu.Lock()
		err := host.closeErr
		host.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (host *Host) shutdown(webClient *WebClient, instances []*Instance) {
	var closeErrors []error
	if webClient != nil {
		if err := webClient.Close(host.ctx); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}
	for _, instance := range instances {
		if err := instance.Close(host.ctx); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}

	host.cancel()
	cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	compiledErr := host.compiled.Close(cleanupContext)
	runtimeErr := host.runtime.Close(cleanupContext)

	host.mu.Lock()
	host.closeErr = errors.Join(
		append(closeErrors, compiledErr, runtimeErr)...,
	)
	host.mu.Unlock()
	close(host.closeDone)
}

func (host *Host) removeWebClient(client *WebClient) {
	host.mu.Lock()
	if host.webClient == client {
		host.webClient = nil
	}
	host.mu.Unlock()
}

func (host *Host) removeInstance(instance *Instance) {
	host.mu.Lock()
	delete(host.instances, instance)
	host.mu.Unlock()
}

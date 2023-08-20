package balancer

import (
	"context"
	"log"
	"strings"
	"time"
)

type Client interface {
	// Weight is unit-less number that determines how much processing capacity can a client be allocated
	// when running in parallel with other clients. The higher the weight, the more capacity the client receives.
	Weight() int
	// Workload returns a channel of work chunks that are ment to be processed through the Server.
	// Client's channel is always filled with work chunks.
	Workload(ctx context.Context) chan int
}

// queriedClient is a client that has been registered to the balancer with context
type queriedClient struct {
	client Client
	ctx    context.Context
}

// workload is a single work chunk (request) that is to be processed by the Server (with context).
type workload struct {
	workChunk int
	ctx       context.Context
}

// Server defines methods required to process client's work chunks (requests).
type Server interface {
	// Process takes one work chunk (request) and does something with it. The error can be ignored.
	Process(ctx context.Context, workChunk int) error
}

// Balancer makes sure the Server is not smashed with incoming requests (work chunks) by only enabling certain number
// of parallel requests processed by the Server. Imagine there's a SLO defined, and we don't want to make the expensive
// service people angry.
//
// If implementing more advanced balancer, ake sure to correctly assign processing capacity to a client based on other
// clients currently in process.
// To give an example of this, imagine there's a maximum number of work chunks set to 100 and there are two clients
// registered, both with the same priority. When they are both served in parallel, each of them gets to send
// 50 chunks at the same time.
// In the same scenario, if there were two clients with priority 1 and one client with priority 2, the first
// two would be allowed to send 25 requests and the other one would send 50. It's likely that the one sending 50 would
// be served faster, finishing the work early, meaning that it would no longer be necessary that those first two
// clients only send 25 each but can and should use the remaining capacity and send 50 again.
type Balancer struct {
	// implement me
	maxParallel             int32               // maximum number of work chunks that can be processed in parallel
	clientRegistrationQueue chan *queriedClient // queue of registered clients
	processQueue            chan workload       // queue of workload for processing (limited by maxLoad)
}

// New creates a new Balancer instance. It needs the server that it's going to balance for and a maximum number of work
// chunks that can the processor process at a time. THIS IS A HARD REQUIREMENT - THE SERVICE CANNOT PROCESS MORE THAN
// <PROVIDED NUMBER> OF WORK CHUNKS IN PARALLEL.
func New(server Server, maxParallel int32) *Balancer {
	b := &Balancer{
		maxParallel:             maxParallel,
		clientRegistrationQueue: make(chan *queriedClient),
		processQueue:            make(chan workload, maxParallel),
	}
	// reading from clientRegistrationQueue and adding work to processQueue
	go func() {
		for {
			select {
			case c := <-b.clientRegistrationQueue:
				go func() {
					log.Default().Println(strings.Repeat("▁", 50))
					log.Default().Printf("Client registered %v", c)
					log.Default().Println(strings.Repeat("▁", 50))
					wl := c.client.Workload(c.ctx)
					for i := 0; i <= c.client.Weight(); i++ {
						go func() {
							for {
								select {
								case load := <-wl:
									if load > 0 {
										go func() {
											b.processQueue <- workload{
												workChunk: load,
												ctx:       c.ctx,
											}
										}()
									} else {
										continue
									}
								default:
								}
							}
						}()
					}
				}()

			default:
				// wait for a second before checking again
				time.Sleep(1 * time.Second)
			}
		}
	}()

	// reading from processQueue and processing clients in parallel (limited by maxLoad)
	for i := 0; i < int(maxParallel); i++ {
		go func() {
			for {
				select {
				case workload := <-b.processQueue:
					go func() {
						server.Process(workload.ctx, workload.workChunk)
					}()
				default:
				}
			}
		}()
	}
	return b
}

// Register a client to the balancer and start processing its work chunks through provided processor (server).
// For the sake of simplicity, assume that the client has no identifier, meaning the same client can register themselves
// multiple times.
func (b *Balancer) Register(ctx context.Context, client Client) {
	// total workChunk registered by clients

	// Registering a client in the clientRegistrationQueue
	b.clientRegistrationQueue <- &queriedClient{
		client: client,
		ctx:    ctx,
	}
}

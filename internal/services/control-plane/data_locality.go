package controlplane

// DataLocalityRegistry tracks the physical location of generated outputs across the cluster.
type DataLocalityRegistry interface {
	RegisterOutput(resourceKey string, workerID string)
	GetProducer(resourceKey string) (string, bool)
	Invalidate(resourceKey string)
}

type regLocRequest struct {
	resourceKey string
	workerID    string
}

type getLocRequest struct {
	resourceKey string
	respCh      chan getLocResponse
}

type getLocResponse struct {
	workerID string
	exists   bool
}

type MemoryDataLocalityRegistry struct {
	regCh chan regLocRequest
	getCh chan getLocRequest
	invCh chan string
}

func (r *MemoryDataLocalityRegistry) RegisterOutput(resourceKey string, workerID string) {
	if resourceKey == "" {
		return
	}
	r.regCh <- regLocRequest{resourceKey, workerID}
}

func (r *MemoryDataLocalityRegistry) GetProducer(resourceKey string) (string, bool) {
	respCh := make(chan getLocResponse, 1)
	r.getCh <- getLocRequest{resourceKey, respCh}
	resp := <-respCh
	return resp.workerID, resp.exists
}

func (r *MemoryDataLocalityRegistry) Invalidate(resourceKey string) {
	r.invCh <- resourceKey
}

func (r *MemoryDataLocalityRegistry) run() {
	resourceToWorker := make(map[string]string)
	for {
		select {
		case req := <-r.regCh:
			resourceToWorker[req.resourceKey] = req.workerID
		case req := <-r.getCh:
			id, ok := resourceToWorker[req.resourceKey]
			req.respCh <- getLocResponse{id, ok}
		case key := <-r.invCh:
			delete(resourceToWorker, key)
		}
	}
}

func NewDataLocalityRegistry() DataLocalityRegistry {
	r := &MemoryDataLocalityRegistry{
		regCh: make(chan regLocRequest, 100),
		getCh: make(chan getLocRequest, 100),
		invCh: make(chan string, 10),
	}
	go r.run()
	return r
}

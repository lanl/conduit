// Copyright 2026. Triad National Security, LLC. All rights reserved.

package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"github.com/google/uuid" // Action stuff
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/etcd"
)

type Job struct {
	JobID            uuid.UUID // value of the item (111-111-111)
	Priority         int64     // priority of item in queue
	SchedulerCommand proto.SchedulerCommand
	Index            int // index of the item in the heap
	CreatedTime      time.Time
	StopCtx          context.CancelFunc
}

// A priority queue implements heap.Interface and holds the Jobs
type PriorityQueue []*Job

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func jobLess(a, b *Job) bool {
	// We want Pop to give us the highest priority
	// which is why we use the greater than symbol
	// priority queue is sorted by these values:
	// 1. validation jobs
	// 2. higher priority value
	// 3. created datetime

	switch {
	case a.SchedulerCommand == proto.SchedulerCommand_VALIDATION && b.SchedulerCommand != proto.SchedulerCommand_VALIDATION:
		return true
	case a.SchedulerCommand != proto.SchedulerCommand_VALIDATION && b.SchedulerCommand == proto.SchedulerCommand_VALIDATION:
		return false
	case a.Priority > b.Priority:
		return true
	case a.Priority < b.Priority:
		return false
	case a.CreatedTime.Before(b.CreatedTime):
		return true
	case b.CreatedTime.Before(a.CreatedTime):
		return false
	default:
		return a.JobID.String() < b.JobID.String()
	}

}

func (pq PriorityQueue) Less(i, j int) bool {
	return jobLess(pq[i], pq[j])
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Job)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // to avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) AddJob(jobID uuid.UUID, schedulerCommand proto.SchedulerCommand, createdTime time.Time, priority int64, em *etcd.ETCDManager) {
	// check for an already existing job in the queue
	for _, existing := range *pq {
		if existing.JobID == jobID &&
			existing.SchedulerCommand == schedulerCommand {
			return
		}
	}

	// create stop context here
	updateExpiryStopCtx, updateExpiryCancel := context.WithCancel(context.Background())

	// Insert a new item and then modify its priority
	item := &Job{
		JobID:            jobID,
		Priority:         priority,
		SchedulerCommand: schedulerCommand,
		CreatedTime:      createdTime,
		StopCtx:          updateExpiryCancel,
	}
	heap.Push(pq, item)

	// Figure out where this job ranks in the queue.
	position := 1
	for _, existing := range *pq {
		if existing != item && jobLess(existing, item) {
			position++
		}
	}

	it := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: jobID.String()})

	// start updating the expiry for the transfer
	if em != nil {
		go em.UpdateExpiryConstantly(it, updateExpiryStopCtx, fmt.Sprintf("queued by scheduler (%d/%d)", position, pq.Len()))
	}
}

func (pq *PriorityQueue) PopJob() (*Job, error) {
	if pq.Len() == 0 {
		return nil, fmt.Errorf("priority queue is empty")
	}
	// Look at priority queue and remove the top value to send to the runners
	top := heap.Pop(pq).(*Job)

	// stop updating the expiry for the transfer
	top.StopCtx()

	return top, nil
}

// requires a lock
func (pq *PriorityQueue) RemoveJob(id uuid.UUID) (newQueue *PriorityQueue) {
	queue := *pq

	for i := 0; i < len(queue); {
		if queue[i].JobID == id {
			// stop updating the expiry for the transfer
			queue[i].StopCtx()

			queue = append(queue[:i], queue[i+1:]...)
			continue
		}

		i++
	}

	heap.Init(&queue)
	return &queue
}

package adapter

import (
	"github.com/DVV-15324/witches-realtime/pkg/uid"
	"sync/atomic"
)

type UIDAdapter struct {
	objectID uint
	counter  uint32
}

func NewUIDAdapter(objectID uint) *UIDAdapter {
	return &UIDAdapter{objectID: objectID}
}

func (a *UIDAdapter) ToBase58() string {
	localID := atomic.AddUint32(&a.counter, 1)
	u := uid.NewUID(localID, a.objectID)
	return u.ToBase58()
}

package gidreclaimer

import (
	"github.com/kubernetes-sigs/sig-storage-lib-external-provisioner/allocator"
)

// GIDReclaimer provides an interface for implementing objects that know how to find a GID that had previously been
// assigned to the directory that was associated with a PVC in the backend storage when the PVC no longer exists but
// the storage was not removed.  For example, an AWS EFS file system has a directory created for each PVC and a GID is
// assigned to that directory if the EFS provisioner is configured to do so.  If the PVC and PV are deleted but the PV
// was configured not to delete the back end storage, then the GID assigned to that directory is still in use.  However,
// the PV is gone, so to figure out the GID that is still in use, the file system has to be inspected.
type GIDReclaimer interface {
	Reclaim(className string, gidTable *allocator.MinMaxAllocator) error
}
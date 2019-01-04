package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"syscall"

	"github.com/golang/glog"
	"github.com/kubernetes-sigs/sig-storage-lib-external-provisioner/allocator"
	"github.com/kubernetes-sigs/sig-storage-lib-external-provisioner/controller"
	"github.com/kubernetes-sigs/sig-storage-lib-external-provisioner/gidreclaimer"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

// compile time check to make sure fileSystemReclaimer implements the GIDReclaimer interface
var _ gidreclaimer.GIDReclaimer = &fileSystemReclaimer{}

func newFileSystemReclaimer(basePath string) *fileSystemReclaimer {
	return &fileSystemReclaimer{basePath}
}

type fileSystemReclaimer struct {
	BasePath string
}

// Reclaim looks at every top level directory in the basepath and adds its gid to the given gidTable
func (f *fileSystemReclaimer) Reclaim(classname string, gidtable *allocator.MinMaxAllocator) error {
	glog.Infof("adding gids for any existing directories under %s to the gid table", f.BasePath)

	entries, err := ioutil.ReadDir(f.BasePath)
	if err != nil {
		glog.Errorf("failed to list contents of %v: %v", f.BasePath, err)
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		mddir := path.Join(f.BasePath, entry.Name())

		md, err := readVolumeMetadata(mddir)
		if err != nil {
			glog.Warningf("failed to read volume metadata for %v: %v", mddir, err)
			continue
		}

		// if no metadata then it must have been created by another storage class that doesn't have reuseVolumes set since those don't write metadata
		if md == nil {
			continue
		}

		// no GID was previously allocated
		if md.GID == "" {
			continue
		}

		// skip volumes for other storage classes
		if md.StorageClassName != classname {
			continue
		}

		gid, err := strconv.Atoi(md.GID)
		if err != nil {
			glog.Errorf("invalid GID value '%v' in metadata for %v", md.GID, mddir)
			continue
		}

		_, err = gidtable.Allocate(gid)
		if err == allocator.ErrConflict {
			glog.Infof("GID %v found in %v was already allocated for storageclass %v", gid, mddir, classname)
			continue
		} else if err != nil {
			glog.Errorf("failed to store GID %v found in metadata for %v: %v", gid, mddir, err)
			continue
		}
	}

	return nil
}

// validatePreexistingVolume determines if the preexisting directory originally came from the new PVC that is being deployed
// based on the contents of the metadata file stored in the directory.  If the storage class, PCV name, PVC namespace, and GID all match,
// then we assume the PVC now being deployed previously must have resulted in this directory being created because the PVC was deleted,
// but the directory wasn't (maybe because the reclaim policy on the storage class was set to Retain, or maybe because the entire Kubernetes
// cluster was destroyed and recreated but the same EFS was reused for the cluster).
func validatePreexistingVolume(options controller.VolumeOptions, md *volumeMetadata, volumePath string, existingGID uint32) error {
	if md == nil {
		msg := fmt.Sprintf("%v already exists but has no volume metadata", volumePath)
		glog.Error(msg)
		return errors.New(msg)
	}

	class := helper.GetPersistentVolumeClaimClass(options.PVC)
	if md.StorageClassName != class {
		msg := fmt.Sprintf("%v already exists but was created for storage class %v instead of the currently requested storage class of %v",
			volumePath, md.StorageClassName, class)
		glog.Error(msg)
		return errors.New(msg)
	}

	if md.PVCName != options.PVC.Name || md.PVCNamespace != options.PVC.Namespace {
		msg := fmt.Sprintf("%v already exists but was created for storage class %v/%v instead of the currently requested storage class of %v/%v",
			volumePath, md.PVCNamespace, md.PVCName, options.PVC.Namespace, class)
		glog.Error(msg)
		return errors.New(msg)
	}

	if md.GID != "" {
		mdgid, _ := md.GidAsUInt()

		if existingGID != mdgid {
			msg := fmt.Sprintf("%v already exists, but its gid is %v while the volume metadata says the gid should be %v",
				volumePath, existingGID, mdgid)
			glog.Error(msg)
			return errors.New(msg)
		}
	}

	return nil
}

// volumeExists determines if the given directory already exists, and if so returns the GID
func volumeExists(path string) (bool, uint32, error) {
	stat, err := os.Stat(path)

	if err != nil {
		if os.IsNotExist(err) {
			return false, 0, nil
		} else {
			glog.Errorf("Failed to determine if %v already exists: %v", path, err)
			return false, 0, err
		}
	} else {
		existingGid := stat.Sys().(*syscall.Stat_t).Gid

		if !stat.IsDir() {
			// not likely to occur unless someone is doing something weird
			msg := fmt.Sprintf("%v already exists but is a file: %v", path, err)
			glog.Error(msg)
			return false, 0, errors.New(msg)
		} else {
			return true, existingGid, nil
		}
	}
}

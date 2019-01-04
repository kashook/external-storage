package main

import (
	"os"
	"path"
	"strconv"

	"encoding/json"
	"github.com/golang/glog"
	"io/ioutil"
)

const (
	metadataFile = ".kube-efs-provisioner-metadata"
)

type volumeMetadata struct {
	GID              string `json:"gid"`
	PVCName          string `json:"pvcName"`
	PVCNamespace     string `json:"pvcNamespace"`
	StorageClassName string `json:"storageClassName"`
}

func (v volumeMetadata) GidAsUInt() (uint32, error) {
	gid, err := strconv.ParseUint(v.GID, 10, 32)
	if err != nil {
		return 0, err
	}

	return uint32(gid), nil
}

// writeVolumeMetadata writes a serialized version of the given volumeMetadata object to the given directory.
func writeVolumeMetadata(dir string, md volumeMetadata) error {
	mdpath := getMetaDataPath(dir)

	contents, err := json.MarshalIndent(md, "", "  ")
	if err != nil {
		glog.Errorf("failed to marshal metadata: %v", err)
		return err
	}

	if err := ioutil.WriteFile(mdpath, contents, 0600); err != nil {
		glog.Errorf("failed to write metadata file %v: %v", mdpath, err)
		return err
	}

	return nil
}

// readVolumeMetadata reads the metadata file in the given directory and returns a *VolumeMetadata with the contents.
func readVolumeMetadata(dir string) (*volumeMetadata, error) {
	md := &volumeMetadata{}
	mdpath := getMetaDataPath(dir)

	content, err := ioutil.ReadFile(mdpath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		} else {
			glog.Errorf("failed to read metadata file %v: %v", mdpath, err)
			return nil, err
		}
	}

	if err := json.Unmarshal(content, md); err != nil {
		glog.Errorf("failed to unmarshal %v: %v", mdpath, err)
		return nil, err
	}

	return md, nil
}

func getMetaDataPath(dir string) string {
	return path.Join(dir, metadataFile)
}

/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/efs"
	"github.com/golang/glog"
	"github.com/kubernetes-sigs/sig-storage-lib-external-provisioner/controller"
	"github.com/kubernetes-sigs/sig-storage-lib-external-provisioner/gidallocator"
	"github.com/kubernetes-sigs/sig-storage-lib-external-provisioner/mount"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

const (
	provisionerNameKey = "PROVISIONER_NAME"
	fileSystemIDKey    = "FILE_SYSTEM_ID"
	awsRegionKey       = "AWS_REGION"
	dnsNameKey         = "DNS_NAME"
)

var _ controller.Provisioner = &efsProvisioner{}

type efsProvisioner struct {
	allocator  gidallocator.Allocator
	dnsName    string
	mountpoint string
	source     string
}

// NewEFSProvisioner creates an AWS EFS volume provisioner
func NewEFSProvisioner(client kubernetes.Interface) controller.Provisioner {
	fileSystemID := os.Getenv(fileSystemIDKey)
	if fileSystemID == "" {
		glog.Fatalf("environment variable %s is not set! Please set it.", fileSystemIDKey)
	}

	awsRegion := os.Getenv(awsRegionKey)
	if awsRegion == "" {
		glog.Fatalf("environment variable %s is not set! Please set it.", awsRegionKey)
	}

	dnsName := os.Getenv(dnsNameKey)
	glog.Errorf("%v", dnsName)
	if dnsName == "" {
		dnsName = getDNSName(fileSystemID, awsRegion)
	}

	mountpoint, source, err := getMount(dnsName)
	if err != nil {
		glog.Fatal(err)
	}

	sess, err := session.NewSession()
	if err != nil {
		glog.Warningf("couldn't create an AWS session: %v", err)
	}

	svc := efs.New(sess, &aws.Config{Region: aws.String(awsRegion)})
	params := &efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(fileSystemID),
	}

	_, err = svc.DescribeFileSystems(params)
	if err != nil {
		glog.Warningf("couldn't confirm that the EFS file system exists: %v", err)
	}

	allocator := gidallocator.NewWithGIDReclaimer(client, newFileSystemReclaimer(mountpoint))

	return &efsProvisioner{
		dnsName:    dnsName,
		mountpoint: mountpoint,
		source:     source,
		allocator:  allocator,
	}
}

func getDNSName(fileSystemID, awsRegion string) string {
	return fileSystemID + ".efs." + awsRegion + ".amazonaws.com"
}

func getMount(dnsName string) (string, string, error) {
	entries, err := mount.GetMounts()
	if err != nil {
		return "", "", err
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Source, dnsName) {
			return e.Mountpoint, e.Source, nil
		}
	}

	entriesStr := ""
	for _, e := range entries {
		entriesStr += e.Source + ":" + e.Mountpoint + ", "
	}
	return "", "", fmt.Errorf("no mount entry found for %s among entries %s", dnsName, entriesStr)
}

func reuseVolumesOption(options controller.VolumeOptions) (bool, error) {
	if reuseStr, ok := options.Parameters["reuseVolumes"]; ok {
		reuse, err := strconv.ParseBool(options.Parameters["reuseVolumes"])
		if err != nil {
			return false, fmt.Errorf("invalid value %s for parameter reuseVolumes: %v", reuseStr, err)
		}
		return reuse, nil
	}
	return false, nil
}

// Provision creates a storage asset and returns a PV object representing it.
func (p *efsProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	if options.PVC.Spec.Selector != nil {
		return nil, fmt.Errorf("claim.Spec.Selector is not supported")
	}

	volumePath, err := p.getLocalPath(options)
	if err != nil {
		glog.Errorf("Failed to provision volume: %v", err)
		return nil, err
	}

	glog.Infof("provisioning volume at %s", volumePath)

	volExists := false
	var existingGid uint32
	var gid *int
	var reuseVolumes bool

	if reuseVolumes, err = reuseVolumesOption(options); err != nil {
		glog.Errorf("%v", err)
		return nil, err
	}

	if reuseVolumes {
		volExists, existingGid, err = volumeExists(volumePath) // existingGid is the actual gid on the directory in the file system
		if err != nil {
			return nil, err
		}
	}

	// hook back up to existing directory if we are configured to reuse volumes, the volume exists, and its metadata matches the current PVC and storageclass.
	if volExists {
		glog.Infof("%s already exists", volumePath)

		md, err := readVolumeMetadata(volumePath)
		if err != nil {
			msg := fmt.Sprintf("failed to read volume metadata for %v: %v", volumePath, err)
			glog.Error(msg)
			return nil, errors.New(msg)
		}

		err = validatePreexistingVolume(options, md, volumePath, existingGid)
		if err != nil {
			return nil, err
		}

		// if a GID was previously allocated and it matches the actual GID on the current directory then use it
		if md.GID != "" {
			existingGidInt := int(existingGid)
			mdGidInt, err := strconv.Atoi(md.GID)
			if err != nil {
				msg := fmt.Sprintf("volume metadata contains an invalid GID value: %v", md.GID)
				glog.Errorf(msg)
				return nil, errors.New(msg)
			}

			if existingGidInt == mdGidInt {
				gid = &existingGidInt
			} else {
				msg := fmt.Sprintf("directory %v has a GID of %v, but the volume metadata shows the GID as %v", volumePath, existingGid, md.GID)
				glog.Error(msg)
				return nil, errors.New(msg)
			}
		}

		glog.Infof("%s was reused since the preexisting volume metadata matches the PVC", volumePath)
	} else {
		gidAllocate := true
		for k, v := range options.Parameters {
			switch strings.ToLower(k) {
			case "gidmin":
				// Let allocator handle
			case "gidmax":
				// Let allocator handle
			case "gidallocate":
				b, err := strconv.ParseBool(v)
				if err != nil {
					return nil, fmt.Errorf("invalid value %s for parameter %s: %v", v, k, err)
				}
				gidAllocate = b
			}
		}

		if gidAllocate {
			allocate, err := p.allocator.AllocateNext(options)
			if err != nil {
				return nil, err
			}
			gid = &allocate
		}

		err := p.createVolume(volumePath, gid)
		if err != nil {
			return nil, err
		}

		var gidstr string
		if gid != nil {
			gidstr = strconv.Itoa(*gid)
		}

		if reuseVolumes {
			writeVolumeMetadata(volumePath,
				volumeMetadata{
					GID:              gidstr,
					PVCName:          options.PVC.Name,
					PVCNamespace:     options.PVC.Namespace,
					StorageClassName: helper.GetPersistentVolumeClaimClass(options.PVC),
				})
		}
	}

	mountOptions := []string{"vers=4.1"}
	if options.MountOptions != nil {
		mountOptions = options.MountOptions
	}

	remotePath, err := p.getRemotePath(options)
	if err != nil {
		glog.Errorf("Failed to get remote path: %v", err)
		return nil, err
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.PVName,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
			AccessModes:                   options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				NFS: &v1.NFSVolumeSource{
					Server:   p.dnsName,
					Path:     remotePath,
					ReadOnly: false,
				},
			},
			MountOptions: mountOptions,
		},
	}

	if gid != nil {
		pv.ObjectMeta.Annotations = map[string]string{
			gidallocator.VolumeGidAnnotationKey: strconv.FormatInt(int64(*gid), 10),
		}
	}

	return pv, nil
}

func (p *efsProvisioner) createVolume(path string, gid *int) error {
	perm := os.FileMode(0777)
	if gid != nil {
		perm = os.FileMode(0771 | os.ModeSetgid)
	}

	if err := os.MkdirAll(path, perm); err != nil {
		return err
	}

	// Due to umask, need to chmod
	if err := os.Chmod(path, perm); err != nil {
		os.RemoveAll(path)
		return err
	}

	if gid != nil {
		cmd := exec.Command("chgrp", strconv.Itoa(*gid), path)
		out, err := cmd.CombinedOutput()
		if err != nil {
			os.RemoveAll(path)
			return fmt.Errorf("chgrp failed with error: %v, output: %s", err, out)
		}
	}

	return nil
}

func (p *efsProvisioner) getLocalPath(options controller.VolumeOptions) (string, error) {
	dirname, err := p.getDirectoryName(options)
	if err != nil {
		return "", err
	}
	return path.Join(p.mountpoint, dirname), nil
}

func (p *efsProvisioner) getRemotePath(options controller.VolumeOptions) (string, error) {
	dirname, err := p.getDirectoryName(options)
	if err != nil {
		return "", err
	}
	sourcePath := path.Clean(strings.Replace(p.source, p.dnsName+":", "", 1))
	return path.Join(sourcePath, dirname), nil
}

// getDirectoryName determines the name of the directory to create for the PVC.
// If we are in "reuse volumes" mode, then we generate a predictable name so that
// the same PVC will always result in the same directory name.  Otherwise, we generate
// a unique name using the name of the generated PV
func (p *efsProvisioner) getDirectoryName(options controller.VolumeOptions) (string, error) {
	reuseVolumes, err := reuseVolumesOption(options)
	if err != nil {
		return "", err
	}

	prefix := ""

	if reuseVolumes {
		if pfx, ok := options.Parameters["volumePrefix"]; ok && pfx != "" {
			prefix = pfx + "-"
		}

		return prefix + options.PVC.Name + "-" + options.PVC.Namespace, nil
	} else {
		return options.PVC.Name + "-" + options.PVName, nil
	}

}

// Delete removes the storage asset that was created by Provision represented
// by the given PV.
func (p *efsProvisioner) Delete(volume *v1.PersistentVolume) error {
	//TODO ignorederror
	err := p.allocator.Release(volume)
	if err != nil {
		return err
	}

	path, err := p.getLocalPathToDelete(volume.Spec.NFS)
	if err != nil {
		return err
	}

	glog.Infof("Deleting %s", path)

	if err := os.RemoveAll(path); err != nil {
		return err
	}

	return nil
}

func (p *efsProvisioner) getLocalPathToDelete(nfs *v1.NFSVolumeSource) (string, error) {
	if nfs.Server != p.dnsName {
		return "", fmt.Errorf("volume's NFS server %s is not equal to the server %s from which this provisioner creates volumes", nfs.Server, p.dnsName)
	}

	sourcePath := path.Clean(strings.Replace(p.source, p.dnsName+":", "", 1))
	if !strings.HasPrefix(nfs.Path, sourcePath) {
		return "", fmt.Errorf("volume's NFS path %s is not a child of the server path %s mounted in this provisioner at %s", nfs.Path, p.source, p.mountpoint)
	}

	subpath := strings.Replace(nfs.Path, sourcePath, "", 1)

	return path.Join(p.mountpoint, subpath), nil
}

// buildKubeConfig builds REST config based on master URL and kubeconfig path.
// If both of them are empty then in cluster config is used.
func buildKubeConfig() (*rest.Config, error) {
	kubeconfig := os.Getenv("KUBECONFIG_PATH")
	master := os.Getenv("KUBE_MASTER_URL")

	if master != "" || kubeconfig != "" {
		glog.Infof("Either master or kubeconfig specified. building kube config from that..")
		return clientcmd.BuildConfigFromFlags(master, kubeconfig)
	} else {
		glog.Infof("Building kube configs for running in cluster...")
		return rest.InClusterConfig()
	}
}

func main() {
	flag.Parse()
	flag.Set("logtostderr", "true")

	glog.Info("Starting efs-provisioner")

	// Create an InClusterConfig and use it to create a client for the controller
	// to use to communicate with Kubernetes
	config, err := buildKubeConfig()
	if err != nil {
		glog.Fatalf("Failed to create config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Failed to create client: %v", err)
	}

	// The controller needs to know what the server version is because out-of-tree
	// provisioners aren't officially supported until 1.5
	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		glog.Fatalf("Error getting server version: %v", err)
	}

	// Create the provisioner: it implements the Provisioner interface expected by
	// the controller
	efsProvisioner := NewEFSProvisioner(clientset)

	provisionerName := os.Getenv(provisionerNameKey)
	if provisionerName == "" {
		glog.Fatalf("environment variable %s is not set! Please set it.", provisionerNameKey)
	}

	// Start the provision controller which will dynamically provision efs NFS
	// PVs
	pc := controller.NewProvisionController(
		clientset,
		provisionerName,
		efsProvisioner,
		serverVersion.GitVersion,
	)

	glog.Info("Starting provisioner controller")

	pc.Run(wait.NeverStop)
}

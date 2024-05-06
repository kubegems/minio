package cephfs

import (
	"context"
	"net"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/minio/cli"
	madmin "github.com/minio/madmin-go"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/internal/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
)

var _ minio.ObjectLayer = (*cephfsObjects)(nil)

func init() {
	const cephfsGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} PATH
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
PATH:
  path to CephFS mount point

EXAMPLES:
  1. Start minio gateway server for CephFS backend
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}secretkey
	 {{.Prompt}} {{.EnvVarSetCommand}} MINIO_K8S_CONFIG{{.AssignmentOperator}}k8sconfig
     {{.Prompt}} {{.HelpName}} /data /data-mirror
`

	selfFlag := []cli.Flag{
		&cli.StringFlag{
			Name:  "metrics",
			Value: ":9567",
			Usage: "address to export metrics",
		},
	}

	minio.RegisterGatewayCommand(cli.Command{
		Name:               minio.CephFSGateway,
		Usage:              "Ceph FileSystem Storage (CephFS)",
		Action:             cephfsGatewayMain,
		CustomHelpTemplate: cephfsGatewayTemplate,
		HideHelpCommand:    true,
		Flags:              selfFlag,
	})
}

func cephfsGatewayMain(ctx *cli.Context) {
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, minio.CephFSGateway, 1)
	}
	args := ctx.Args()
	if len(args) < 2 {
		cli.ShowCommandHelpAndExit(ctx, minio.CephFSGateway, 1)
	}
	minio.StartGateway(ctx, &CephFS{
		cephfsMountPoint: strings.TrimSuffix(args.Get(0), "/"),
		dataPath:         strings.TrimSuffix(args.Get(1), "/"),
		metricsAddress:   ctx.String("metrics"),
		includeNamespace: "kubegems-pai",
		mountPrefix:      "/volumes/csi/",
	})
}

type CephFS struct {
	cephfsMountPoint string
	dataPath         string
	includeNamespace string
	mountPrefix      string
	metricsAddress   string
}

func (c *CephFS) Name() string {
	return minio.CephFSGateway
}

func (c *CephFS) NewGatewayLayer(creds madmin.Credentials) (minio.ObjectLayer, error) {
	var err error
	metaDir := path.Join(c.cephfsMountPoint, ".minio.sys")
	if err = os.MkdirAll(metaDir, 0o755); err != nil {
		return nil, err
	}
	//创建镜像目录
	if err = os.MkdirAll(c.dataPath, 0o755); err != nil {
		return nil, err
	}
	mirrorMetaDir := path.Join(c.dataPath, ".minio.sys")
	if _, err = os.Stat(mirrorMetaDir); os.IsNotExist(err) {
		err = os.Symlink(metaDir, mirrorMetaDir)
		if err != nil {
			return nil, err
		}
	}

	k8sConfig := os.Getenv("MINIO_K8S_CONFIG")
	cli, err := newClient(k8sConfig)
	if err != nil {
		return nil, err
	}
	go cli.watchPV(c)
	newObject, err := minio.NewFSObjectLayer(c.dataPath)
	if err != nil {
		return nil, err
	}
	exposeMetrics(c.dataPath, c.metricsAddress)
	return &cephfsObjects{newObject}, nil
}

func exposeMetrics(mp, addr string) {
	registry := prometheus.NewRegistry() // replace default so only JuiceFS metrics are exposed
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_",
		prometheus.WrapRegistererWith(prometheus.Labels{"mp": mp}, registry))
	registerer.MustRegister(collectors.NewGoCollector())
	registerer.MustRegister(&s3bucketCollector{})
	ip, port, err := net.SplitHostPort(addr)
	if err != nil {
		logger.LogIf(context.Background(), err)
		return
	}
	http.Handle("/metrics", promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{
			EnableOpenMetrics: true,
		},
	))
	ln, err := net.Listen("tcp", net.JoinHostPort(ip, port))
	if err != nil {
		logger.LogIf(context.Background(), err)
		return
	}
	go func() {
		if err := http.Serve(ln, nil); err != nil {
			logger.LogIf(context.Background(), err)
			return
		}
	}()
}

type cephfsObjects struct {
	minio.ObjectLayer
}

func (c *cephfsObjects) IsListenSupported() bool {
	return false
}

func (c *cephfsObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo, _ []error) {
	si, errs := c.ObjectLayer.StorageInfo(ctx)
	si.Backend.GatewayOnline = si.Backend.Type == madmin.FS
	si.Backend.Type = madmin.Gateway
	return si, errs
}

// 不支持创建桶
func (c *cephfsObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	return minio.NotImplemented{}
}

// 不支持删除桶
func (c *cephfsObjects) DeleteBucket(ctx context.Context, bucket string, opts minio.DeleteBucketOptions) error {
	return minio.NotImplemented{}
}

func (c *cephfsObjects) IsTaggingSupported() bool {
	return true
}

func (c *CephFS) createSymlink(bucket, destPath string, needCheck bool) error {
	var err error
	dataPath := path.Join(c.cephfsMountPoint, destPath)
	if needCheck {
		if _, err = os.Stat(dataPath); err != nil {
			return err
		}
	}
	mirrorPath := path.Join(c.dataPath, bucket)
	if _, err = os.Stat(mirrorPath); os.IsNotExist(err) {
		return os.Symlink(dataPath, mirrorPath)
	}
	return err
}

func (c *CephFS) OnAdd(obj interface{}) {
	pv, ok := obj.(*corev1.PersistentVolume)
	if !ok {
		return
	}
	if pv.Spec.ClaimRef == nil {
		return
	}
	if pv.Spec.ClaimRef.Namespace != c.includeNamespace {
		return
	}
	bucketName := pv.Spec.ClaimRef.Name
	if pv.Spec.CSI == nil {
		return
	}
	path, ok := pv.Spec.CSI.VolumeAttributes["subvolumePath"]
	if !ok {
		return
	}
	path = strings.TrimPrefix(path, c.mountPrefix)
	err := c.createSymlink(bucketName, path, true)
	if err != nil {
		logger.Error("create symlink error:", err)
	}

}
func (c *CephFS) OnDelete(obj interface{}) {
	pv, ok := obj.(*corev1.PersistentVolume)
	if !ok {
		return
	}
	if pv.Spec.ClaimRef == nil {
		return
	}
	if pv.Spec.ClaimRef.Namespace != c.includeNamespace {
		return
	}
	bucketName := pv.Spec.ClaimRef.Name
	mirrorPath := path.Join(c.dataPath, bucketName)
	err := os.Remove(mirrorPath)
	if err != nil {
		logger.Error("remove symlink error:", err)
	}
}
func (c *CephFS) OnUpdate(oldObj, newObj interface{}) {
	oldPV, ok := oldObj.(*corev1.PersistentVolume)
	if !ok {
		return
	}
	newPV, ok := oldObj.(*corev1.PersistentVolume)
	if !ok {
		return
	}
	logger.Info("update pv:", oldPV.Name, "->", newPV.Name)

}

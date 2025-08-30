package cephfs

import (
	"context"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/internal/logger"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

type k8sClient struct {
	kubernetes.Interface
}

func (k *k8sClient) watchPV(h cache.ResourceEventHandler) {
	pvWatcher := cache.NewListWatchFromClient(k.CoreV1().RESTClient(), "persistentvolumes", corev1.NamespaceAll, fields.Everything())
	s, c := cache.NewInformer(pvWatcher, &corev1.PersistentVolume{}, 0, h)
	var (
		sig  = make(chan os.Signal, 1)
		stop = make(chan struct{})
	)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		close(stop)
	}()
	c.Run(stop)
	s.List()
}

func newClient(configPath string) (*k8sClient, error) {
	var (
		config *rest.Config
		err    error
	)
	if configPath != "" {
		kubeconfig, err := os.ReadFile(configPath)
		if err != nil {
			return nil, err
		}
		config, err = clientcmd.RESTConfigFromKubeConfig(kubeconfig)
		if err != nil {
			return nil, err
		}
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
	}

	if config == nil {
		return nil, status.Error(codes.NotFound, "Can't get kube InClusterConfig")
	}
	config.Timeout = 10 * time.Second

	if os.Getenv("KUBE_QPS") != "" {
		kubeQPSInt, err := strconv.Atoi(os.Getenv("KUBE_QPS"))
		if err != nil {
			return nil, err
		}
		config.QPS = float32(kubeQPSInt)
	}
	if os.Getenv("KUBE_BURST") != "" {
		kubeBurstInt, err := strconv.Atoi(os.Getenv("KUBE_BURST"))
		if err != nil {
			return nil, err
		}
		config.Burst = kubeBurstInt
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &k8sClient{client}, nil
}

type s3bucketCollector struct {
	api minio.ObjectLayer
}

func (s *s3bucketCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("s3_request_count", "Total number of requests to S3", []string{"bucket"}, nil)
	ch <- prometheus.NewDesc("s3_input_bytes", "Total number of bytes transferred to S3", []string{"bucket"}, nil)
	ch <- prometheus.NewDesc("total_bytes", "Total S3 storage bytes", []string{"bucket"}, nil)
	ch <- prometheus.NewDesc("total_files", "Total S3 storage files", []string{"bucket"}, nil)
}
func (s *s3bucketCollector) Collect(ch chan<- prometheus.Metric) {
	bts := minio.GlobalBucketStatInfoCount.S3InputBytes.Load()
	for k, v := range bts {
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("s3_input_bytes", "Total number of bytes transferred to S3", []string{"bucket"}, nil), prometheus.CounterValue, float64(v), k)
	}
	rts := minio.GlobalBucketStatInfoCount.S3RequestCount.Load()
	for k, v := range rts {
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("s3_request_count", "Total number of requests to S3", []string{"bucket"}, nil), prometheus.CounterValue, float64(v), k)
	}
	for k, v := range s.api.GetBucketUsage() {
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("total_bytes", "Total S3 storage bytes", []string{"bucket"}, nil), prometheus.GaugeValue, float64(v.Size), k)
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("total_files", "Total S3 storage files", []string{"bucket"}, nil), prometheus.GaugeValue, float64(v.ObjectsCount), k)
	}
}

func (c *cephfsObjects) Scanner(ctx context.Context) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	time.Sleep(time.Second)
	for {
		bucketUsage := make(chan bucketUsageInfo, 10)
		go func() {
			for usage := range bucketUsage {
				c.updateBucketUsage(usage)
			}
		}()
		if err := c.scannerUsage(ctx, bucketUsage); err != nil {
			logger.LogIf(ctx, err)
		}
		time.Sleep(time.Duration(20+r.Intn(5)) * time.Second)
	}
}

func (c *cephfsObjects) scannerUsage(ctx context.Context, ch chan<- bucketUsageInfo) error {
	defer close(ch)
	buckets, err := c.ListBuckets(ctx)
	if err != nil {
		return err
	}
	fsPath := c.ObjectLayer.(*minio.FSObjects).GetFSPath()
	for _, b := range buckets {
		bu := bucketUsageInfo{
			bucketName: b.Name,
		}
		bucketPath := path.Join(fsPath, b.Name)
		realPath, err := os.Readlink(bucketPath)
		if err != nil {
			fmt.Println("Readlink error:", err)
			continue
		}
		err = filepath.WalkDir(realPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				// 处理访问错误，跳过继续
				fmt.Printf("Warning: Error accessing %s: %v\n", path, err)
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return nil
			}
			if info.IsDir() && (info.Name() == "." || info.Name() == "..") {
				return filepath.SkipDir
			}
			// 只统计普通文件，跳过目录
			if !info.IsDir() {
				bu.ObjectsCount += 1
				bu.Size += uint64(info.Size())
			}
			return nil
		})
		if err != nil {
			logger.Error("run filepath.WalkDir error:", err)
			continue
		}
		fmt.Println("bucketUsageInfo:", bu)
		ch <- bu
	}
	return nil
}

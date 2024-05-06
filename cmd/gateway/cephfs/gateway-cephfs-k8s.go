package cephfs

import (
	"context"
	"os"
	"os/signal"
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

type s3bucketCollector struct{}

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

	usage, err := minio.LoadDataUsageFromBackendForCephFS(context.Background())
	if err != nil {
		logger.LogIf(context.Background(), err)
		return
	}
	for k, v := range usage.BucketsUsage {
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("total_bytes", "Total S3 storage bytes", []string{"bucket"}, nil), prometheus.GaugeValue, float64(v.Size), k)
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("total_files", "Total S3 storage files", []string{"bucket"}, nil), prometheus.GaugeValue, float64(v.ObjectsCount), k)
	}
}

// func (k *k8sClient) executeInContainer(podName, namespace, containerName string, cmd []string) (stdout string, stderr string, err error) {
// 	klog.V(6).Infof("Execute command %v in container %s in pod %s in namespace %s", cmd, containerName, podName, namespace)
// 	const tty = false

// 	req := k.CoreV1().RESTClient().Post().
// 		Resource("pods").
// 		Name(podName).
// 		Namespace(namespace).
// 		SubResource("exec").
// 		Param("container", containerName)
// 	req.VersionedParams(&corev1.PodExecOptions{
// 		Container: containerName,
// 		Command:   cmd,
// 		Stdin:     false,
// 		Stdout:    true,
// 		Stderr:    true,
// 		TTY:       tty,
// 	}, scheme.ParameterCodec)

// 	var sout, serr bytes.Buffer
// 	config, err := rest.InClusterConfig()
// 	if err != nil {
// 		return "", "", err
// 	}
// 	config.Timeout = timeout
// 	err = execute("POST", req.URL(), config, nil, &sout, &serr, tty)

// 	return strings.TrimSpace(sout.String()), strings.TrimSpace(serr.String()), err
// }

// func execute(method string, url *url.URL, config *restclient.Config, stdin io.Reader, stdout, stderr io.Writer, tty bool) error {
// 	exec, err := remotecommand.NewSPDYExecutor(config, method, url)
// 	if err != nil {
// 		return err
// 	}
// 	return exec.Stream(remotecommand.StreamOptions{
// 		Stdin:  stdin,
// 		Stdout: stdout,
// 		Stderr: stderr,
// 		Tty:    tty,
// 	})
// }

package cephfs

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

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

// func (k *K8sClient) ExecuteInContainer(podName, namespace, containerName string, cmd []string) (stdout string, stderr string, err error) {
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

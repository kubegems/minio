package juicefs

import (
	"os"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/minio/cli"
	"github.com/prometheus/client_golang/prometheus"
)

type bucketMetrics struct {
	valid      bool
	usedSpaceG prometheus.Gauge
	usedFilesG prometheus.Gauge
	usedDirsG  prometheus.Gauge
}

func InitMuiltiBucketMetrics(c *cli.Context, registerer prometheus.Registerer, m meta.Meta) {
	if !c.Bool("multi-buckets") {
		return
	}
	bm := &MultiBucketMetrics{
		m:          m,
		buckets:    make(map[string]*bucketMetrics),
		registerer: registerer,
		ctx:        meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())}),
	}
	go bm.run()
}

type MultiBucketMetrics struct {
	registerer prometheus.Registerer
	m          meta.Meta
	buckets    map[string]*bucketMetrics
	tmpsum     meta.Summary
	ctx        meta.Context
}

func (mb *MultiBucketMetrics) run() {
	for {
		select {
		case <-mb.ctx.Done():
			return
		default:
			mb.refresh()
			utils.SleepWithJitter(time.Second * 10)
		}
	}
}

func (mb *MultiBucketMetrics) refresh() error {
	var entries []*meta.Entry
	if err := mb.m.Readdir(mb.ctx, meta.RootInode, 1, &entries); err != 0 {
		return err
	}
	for _, v := range mb.buckets {
		v.valid = false
	}
	// skip . and ..
	entries = entries[2:]
	for _, entry := range entries {
		// skip not directory
		if entry.Attr.Typ != meta.TypeDirectory {
			continue
		}
		name := string(entry.Name)
		metrics, ok := mb.buckets[name]
		if !ok {
			lb := map[string]string{"bucket": name}
			metrics = &bucketMetrics{
				usedSpaceG: prometheus.NewGauge(prometheus.GaugeOpts{Name: "total_bytes", ConstLabels: lb}),
				usedFilesG: prometheus.NewGauge(prometheus.GaugeOpts{Name: "total_files", ConstLabels: lb}),
				usedDirsG:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "total_dirs", ConstLabels: lb}),
			}
			mb.registerer.Register(metrics.usedSpaceG)
			mb.registerer.Register(metrics.usedFilesG)
			mb.registerer.Register(metrics.usedDirsG)
			mb.buckets[name] = metrics
		}
		// get summary use cache
		mb.tmpsum.Dirs = 0
		mb.tmpsum.Files = 0
		mb.tmpsum.Size = 0
		mb.tmpsum.Length = 0
		if err := mb.m.GetSummary(mb.ctx, entry.Inode, &mb.tmpsum, true, false); err != 0 {
			metrics.valid = false
			return err
		}
		// update metrics
		mb.buckets[name].usedSpaceG.Set(float64(mb.tmpsum.Size))
		mb.buckets[name].usedFilesG.Set(float64(mb.tmpsum.Files))
		mb.buckets[name].usedDirsG.Set(float64(mb.tmpsum.Dirs))
		metrics.valid = true
	}
	// remove not valid buckets
	for k, v := range mb.buckets {
		if !v.valid {
			mb.registerer.Unregister(v.usedSpaceG)
			mb.registerer.Unregister(v.usedFilesG)
			mb.registerer.Unregister(v.usedDirsG)
			delete(mb.buckets, k)
		}
	}
	return nil
}

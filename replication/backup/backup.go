package backup

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/wandera/regatta/proto"
	"github.com/wandera/regatta/replication/snapshot"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const manifestFileName = "manifest.json"

type Clock interface {
	Now() time.Time
}

type monotonic struct{}

func (monotonic) Now() time.Time {
	return time.Now()
}

type Logger interface {
	Println(args ...interface{})
	Printf(msg string, args ...interface{})
}

type nilLogger struct{}

func (nilLogger) Println(args ...interface{}) {
	fmt.Println(args...)
}

func (nilLogger) Printf(msg string, args ...interface{}) {
	fmt.Printf(msg, args...)
}

// Manifest a backup manifest containing a backup info.
type Manifest struct {
	Started  time.Time       `json:"started"`
	Finished time.Time       `json:"finished"`
	Tables   []ManifestTable `json:"tables"`
}

// ManifestTable a backup table descriptor.
type ManifestTable struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	FileName string `json:"file_name"`
	MD5      string `json:"md5"`
}

type manifestTables []ManifestTable

func (m manifestTables) Len() int {
	return len(m)
}

func (m manifestTables) Less(i, j int) bool {
	return m[i].Name < m[j].Name
}

func (m manifestTables) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

type Backup struct {
	Conn    *grpc.ClientConn
	Log     Logger
	Timeout time.Duration
	Dir     string
	clock   Clock
}

func (b *Backup) ensureDefaults() {
	if b.Timeout == 0 {
		b.Timeout = 1 * time.Hour
	}
	if b.Log == nil {
		b.Log = nilLogger{}
	}
	if b.clock == nil {
		b.clock = monotonic{}
	}
}

func (b *Backup) Backup() (Manifest, error) {
	b.ensureDefaults()

	mc := proto.NewMetadataClient(b.Conn)
	sc := proto.NewMaintenanceClient(b.Conn)

	manifest := Manifest{
		Started: b.clock.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), b.Timeout)
	defer cancel()

	if b.Dir != "" {
		stat, err := os.Stat(b.Dir)
		if err != nil {
			return manifest, err
		}
		if !stat.IsDir() {
			return manifest, errors.New("argument 'dir' is not a directory")
		}
	}

	meta, err := mc.Get(ctx, &proto.MetadataRequest{})
	if err != nil {
		return manifest, err
	}

	var wg errgroup.Group
	var mtx sync.Mutex
	b.Log.Printf("going to backup %v \n", meta.Tables)
	for _, table := range meta.Tables {
		t := table
		wg.Go(func() error {
			b.Log.Printf("downloading %s \n", t.Name)
			stream, err := sc.Backup(ctx, &proto.BackupRequest{Table: []byte(t.Name)})
			if err != nil {
				return err
			}
			fName := fmt.Sprintf("%s.bak", t.Name)
			sf, err := snapshot.New(filepath.Join(b.Dir, fName))
			if err != nil {
				return err
			}

			hash := md5.New()
			w := io.MultiWriter(hash, sf)
			_, err = io.Copy(w, snapshot.Reader{Stream: stream})
			if err != nil {
				return err
			}
			err = sf.Sync()
			if err != nil {
				return err
			}

			func() {
				mtx.Lock()
				defer mtx.Unlock()
				manifest.Tables = append(manifest.Tables, ManifestTable{
					Name:     t.Name,
					Type:     t.Type.String(),
					FileName: fName,
					MD5:      hex.EncodeToString(hash.Sum(nil)),
				})
			}()
			b.Log.Printf("downloaded %s \n", t.Name)
			return nil
		})
	}
	err = wg.Wait()
	if err != nil {
		return manifest, err
	}
	sort.Sort(manifestTables(manifest.Tables))
	manifest.Finished = b.clock.Now()

	b.Log.Println("tables backed up, writing manifest")
	manFile, err := os.Create(filepath.Join(b.Dir, manifestFileName))
	if err != nil {
		return manifest, err
	}
	defer func() {
		_ = manFile.Close()
	}()

	err = json.NewEncoder(manFile).Encode(manifest)
	if err != nil {
		return manifest, err
	}
	err = manFile.Sync()
	if err != nil {
		return manifest, err
	}
	b.Log.Println("backup complete")
	return manifest, nil
}

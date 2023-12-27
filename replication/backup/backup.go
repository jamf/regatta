// Copyright JAMF Software, LLC

package backup

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jamf/regatta/regattapb"
	"github.com/jamf/regatta/replication/snapshot"
	"google.golang.org/grpc"
)

const (
	manifestFileName         = "manifest.json"
	defaultSnapshotChunkSize = 2 * 1024 * 1024
)

type Clock interface {
	Now() time.Time
}

type monotonic struct{}

func (monotonic) Now() time.Time {
	return time.Now()
}

type Logger interface {
	Info(args ...interface{})
	Infof(msg string, args ...interface{})
}

type nilLogger struct{}

func (nilLogger) Info(args ...interface{}) {
	fmt.Println(args...)
}

func (nilLogger) Infof(msg string, args ...interface{}) {
	fmt.Printf(msg+"\n", args...)
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

	mc := regattapb.NewClusterClient(b.Conn)
	sc := regattapb.NewMaintenanceClient(b.Conn)

	manifest := Manifest{
		Started: b.clock.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), b.Timeout)
	defer cancel()

	err := checkDir(b.Dir)
	if err != nil {
		return manifest, err
	}

	status, err := mc.Status(ctx, &regattapb.StatusRequest{})
	if err != nil {
		return manifest, err
	}

	b.Log.Infof("starting backup")
	for t := range status.Tables {
		b.Log.Infof("backing up table '%s'", t)
		stream, err := sc.Backup(ctx, &regattapb.BackupRequest{Table: []byte(t)})
		if err != nil {
			return manifest, err
		}
		fName := fmt.Sprintf("%s.bak", t)
		sf, err := os.Create(filepath.Join(b.Dir, fName))
		if err != nil {
			return manifest, err
		}

		hash := md5.New()
		w := io.MultiWriter(hash, sf)
		_, err = io.Copy(w, snapshot.Reader{Stream: stream})
		if err != nil {
			return manifest, err
		}
		err = sf.Sync()
		if err != nil {
			return manifest, err
		}

		manifest.Tables = append(manifest.Tables, ManifestTable{
			Name:     t,
			FileName: fName,
			MD5:      hex.EncodeToString(hash.Sum(nil)),
		})
		b.Log.Infof("backed up table '%s'", t)
	}
	sort.Sort(manifestTables(manifest.Tables))
	manifest.Finished = b.clock.Now()

	b.Log.Info("tables backed up, writing manifest")
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
	b.Log.Info("backup complete")
	return manifest, nil
}

func (b *Backup) Restore() error {
	b.ensureDefaults()

	sc := regattapb.NewMaintenanceClient(b.Conn)

	ctx, cancel := context.WithTimeout(context.Background(), b.Timeout)
	defer cancel()

	err := checkDir(b.Dir)
	if err != nil {
		return err
	}

	manFile, err := os.Open(filepath.Join(b.Dir, manifestFileName))
	if err != nil {
		return err
	}
	defer func() {
		_ = manFile.Close()
	}()

	manifest := Manifest{}
	err = json.NewDecoder(manFile).Decode(&manifest)
	if err != nil {
		return err
	}
	b.Log.Info("manifest loaded")

	b.Log.Infof("going to restore %v", manifest.Tables)

	hash := md5.New()
	for _, table := range manifest.Tables {
		hash.Reset()
		tf, err := os.Open(filepath.Join(b.Dir, table.FileName))
		if err != nil {
			return err
		}
		_, err = io.Copy(hash, tf)
		if err != nil {
			return err
		}
		if hex.EncodeToString(hash.Sum(nil)) != table.MD5 {
			return fmt.Errorf("table '%s' file '%s' corrupted (checksum mismatch)", table.Name, table.FileName)
		}
		b.Log.Infof("table '%s' checksum valid", table.Name)
		_, err = tf.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		stream, err := sc.Restore(ctx)
		if err != nil {
			return err
		}
		err = stream.Send(&regattapb.RestoreMessage{
			Data: &regattapb.RestoreMessage_Info{
				Info: &regattapb.RestoreInfo{
					Table: []byte(table.Name),
				},
			},
		})
		if err != nil {
			return err
		}
		b.Log.Infof("table '%s' stream started", table.Name)

		_, err = io.Copy(&Writer{Sender: stream}, bufio.NewReaderSize(tf, defaultSnapshotChunkSize))
		if err != nil {
			return err
		}

		b.Log.Infof("table '%s' streamed", table.Name)
		_, err = stream.CloseAndRecv()
		if err != nil {
			return err
		}
		b.Log.Infof("table '%s' restored", table.Name)
	}

	return nil
}

func checkDir(dir string) error {
	if dir != "" {
		stat, err := os.Stat(dir)
		if err != nil {
			return err
		}
		if !stat.IsDir() {
			return fmt.Errorf("'%s' is not a directory", dir)
		}
	}
	return nil
}

type Writer struct {
	Sender regattapb.Maintenance_RestoreClient
}

func (g Writer) Write(p []byte) (int, error) {
	ln := len(p)
	if err := g.Sender.Send(&regattapb.RestoreMessage{
		Data: &regattapb.RestoreMessage_Chunk{
			Chunk: &regattapb.SnapshotChunk{
				Data: p,
				Len:  uint64(ln),
			},
		},
	}); err != nil {
		return 0, err
	}
	return ln, nil
}

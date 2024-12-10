package storage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

    uuidp "github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	var s *Storage

	t.Log("Create new storage")
	{
		var err error
		workdir := t.TempDir()

		s, err = NewStorage(workdir)
		require.NoError(t, err, "Must init new storage")

		s, err = NewStorage(workdir)
		require.NoError(t, err, "Must reinit new storage")
	}

	t.Log("Test utility methods")
	{
		testID := 0

		t.Logf("\tTest %d:\tTest IsExist method", testID)
		{
			uuid := uuidp.Generate().String()

			require.False(t, s.IsFileExist(uuid), "Must return false for not existing file")

			err := s.WriteFile(uuid, strings.NewReader("hello"))
			require.NoError(t, err, "Must write file")

			require.True(t, s.IsFileExist(uuid), "Must return true for existing file")
		}

		testID++

		t.Logf("\tTest %d:\tTest path method", testID)
		{
			expected_path := filepath.Join(s.Workdir, "files", "3/a/3adc6469-2691-4ba4-8245-94b0c30b15ef")
			path := s.filePath("3adc6469-2691-4ba4-8245-94b0c30b15ef")
			require.Equal(t, expected_path, path, "Return not exppected path")
		}
	}

	t.Log("Test WriteFile method")
	{
		testID := 0
		t.Logf("\tTest %d:\tWrite file", testID)
		{
			text := "Test text"
			uuid := uuidp.Generate().String()
			src := strings.NewReader(text)

			err := s.WriteFile(uuid, src)
			require.NoError(t, err, "Must write file")

			f, err := os.Open(s.filePath(uuid))
			require.NoError(t, err, "Must open writed file")

			b, err := io.ReadAll(f)
			require.NoError(t, err, "Must read everything from the writed file")
			require.Equal(t, string(b), text, "The read string must be equal to the original string")
		}

		testID++
		t.Logf("\tTest %d:\tTry overwrite existing file", testID)
		{
			uuid := uuidp.Generate().String()
			src := strings.NewReader("Test")

			err := s.WriteFile(uuid, src)
			require.NoError(t, err, "Must write file")

			err = s.WriteFile(uuid, src)
			require.ErrorIs(t, err, os.ErrExist, "Must return error when try overwrite file")
		}
	}

	t.Log("Test ReadFile method")
	{
		testID := 0
		t.Logf("\tTest %d:\tRead file", testID)
		{
			text := "My text"
			uuid := uuidp.Generate().String()

			err := s.WriteFile(uuid, strings.NewReader(text))
			require.NoError(t, err, "Must write file")

			buf := new(bytes.Buffer)
			err = s.ReadFile(uuid, buf)
			require.NoError(t, err, "Must read file")
			require.Equal(t, buf.String(), text, "The read string must be equal to the original string")
		}

		testID++
		t.Logf("\tTest %d:\tRead not existing file", testID)
		{
			err := s.ReadFile(uuidp.Generate().String(), nil)
			require.ErrorIs(t, err, os.ErrNotExist, "Must return NotExist error")
		}
	}

	t.Log("Test RemoveFile method")
	{
		testID := 0
		t.Logf("\tTest %d:\tRemove file", testID)
		{
			text := "Test text"
			uuid := uuidp.Generate().String()
			src := strings.NewReader(text)

			err := s.WriteFile(uuid, src)
			require.NoError(t, err, "Must write file")

			err = s.RemoveFile(uuid)
			require.NoError(t, err, "Must remove file")

			f, err := os.Open(s.removedfilePath(uuid))
			require.NoError(t, err, "Must open removed file")

			b, err := io.ReadAll(f)
			require.NoError(t, err, "Must read everything from the removed file")
			require.Equal(t, string(b), text, "The read string must be equal to the original string")
		}

		testID++
		t.Logf("\tTest %d:\tTry remove not existing file", testID)
		{
			err := s.RemoveFile(uuidp.Generate().String())
			require.ErrorIs(t, err, os.ErrNotExist, "Must return NotExist error")
		}
	}
}

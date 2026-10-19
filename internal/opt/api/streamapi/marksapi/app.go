package marksapi

import (
	"time"

	"github.com/pgrwl/pgrwl/internal/opt/marks"
	st "github.com/pgrwl/pgrwl/internal/opt/shared/storecrypt"
)

type Opts struct {
	Store      *marks.Store
	WALStor    *st.VariadicStorage
	WALTimeout time.Duration
}

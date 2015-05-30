package entry

import (
	"fmt"
	"io"
)

type response struct {
	status  string
	msg     string
	noreply bool
	items   map[string]*item
}

func writeFull(w io.Writer, buf []byte) error {
	n, e := w.Write(buf)
	for e != nil && n > 0 {
		buf = buf[n:]
		n, e = w.Write(buf)
	}
	return e
}

func (resp *response) Write(w io.Writer) error {
	if resp.noreply {
		return nil
	}

	switch resp.status {
	case "VALUE":
		if resp.items != nil {
			for key, item := range resp.items {
				fmt.Fprintf(w, "VALUE %s %d %d\r\n", key, item.flag, len(item.body))
				if e := writeFull(w, item.body); e != nil {
					return e
				}
				writeFull(w, []byte("\r\n"))
			}
		}
		io.WriteString(w, "END\r\n")

	case "STAT":
		io.WriteString(w, resp.msg)
		io.WriteString(w, "\r\n")
		io.WriteString(w, "END\r\n")

	default:
		io.WriteString(w, resp.status)
		if resp.msg != "" {
			io.WriteString(w, " "+resp.msg)
		}
		io.WriteString(w, "\r\n")
	}
	return nil
}

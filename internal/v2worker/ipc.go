package v2worker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
)

const MaxIPCFrameBytes = 4 << 20

var (
	ErrIPCFrameTooLarge = errors.New("worker IPC frame exceeds limit")
	ErrIPCFrameInvalid  = errors.New("worker IPC frame is invalid")
)

type IPCRequest struct {
	RequestID       string          `json:"requestId"`
	Operation       string          `json:"operation"`
	Payload         json.RawMessage `json:"payload"`
	DeadlineAt      string          `json:"deadlineAt"`
	Budget          OperationBudget `json:"budget"`
	ArtifactID      string          `json:"artifactId"`
	PackageRelease  string          `json:"packageReleaseId"`
	SessionEpoch    int64           `json:"sessionEpoch"`
	SessionRevision int64           `json:"sessionRevision"`
}

type IPCResponse struct {
	RequestID     string           `json:"requestId"`
	Output        json.RawMessage  `json:"output,omitempty"`
	Error         *StructuredError `json:"error,omitempty"`
	RequestCount  int              `json:"requestCount,omitempty"`
	ResponseBytes int64            `json:"responseBytes,omitempty"`
}

type IPCHandler func(context.Context, IPCRequest) IPCResponse

func WriteIPCFrame(writer io.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return WriteIPCBytes(writer, payload)
}

func WriteIPCBytes(writer io.Writer, payload []byte) error {
	if len(payload) == 0 || len(payload) > MaxIPCFrameBytes {
		return ErrIPCFrameTooLarge
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	if _, err := writeAll(writer, header); err != nil {
		return err
	}
	_, err := writeAll(writer, payload)
	return err
}

func ReadIPCFrame(reader io.Reader) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header)
	if length == 0 || length > MaxIPCFrameBytes {
		return nil, ErrIPCFrameTooLarge
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func ServeIPC(ctx context.Context, reader io.Reader, writer io.Writer, handler IPCHandler) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if handler == nil {
		return ErrIPCFrameInvalid
	}
	for {
		payload, err := ReadIPCFrame(reader)
		if err != nil {
			return err
		}
		var request IPCRequest
		decoder := json.NewDecoder(bytesReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil || request.RequestID == "" || request.Operation == "" {
			return ErrIPCFrameInvalid
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		response := handler(ctx, request)
		if response.RequestID == "" {
			response.RequestID = request.RequestID
		}
		if err := WriteIPCFrame(writer, response); err != nil {
			return err
		}
	}
}

type WorkerProcess struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	mu      sync.Mutex
}

func StartWorkerProcess(ctx context.Context, executable string, args ...string) (*WorkerProcess, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if executable == "" {
		return nil, ErrWorkerInvalid
	}
	command := exec.CommandContext(ctx, executable, args...)
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	return &WorkerProcess{command: command, stdin: stdin, stdout: stdout}, nil
}

func (process *WorkerProcess) Call(ctx context.Context, request IPCRequest) (IPCResponse, error) {
	if process == nil || process.stdin == nil || process.stdout == nil {
		return IPCResponse{}, ErrWorkerInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	process.mu.Lock()
	defer process.mu.Unlock()
	if err := WriteIPCFrame(process.stdin, request); err != nil {
		return IPCResponse{}, err
	}
	payload, err := readIPCFrameWithContext(ctx, process.stdout)
	if err != nil {
		return IPCResponse{}, err
	}
	var response IPCResponse
	decoder := json.NewDecoder(bytesReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil || response.RequestID != request.RequestID {
		return IPCResponse{}, ErrIPCFrameInvalid
	}
	return response, nil
}

func (process *WorkerProcess) Close() error {
	if process == nil {
		return nil
	}
	if process.stdin != nil {
		_ = process.stdin.Close()
	}
	if process.stdout != nil {
		_ = process.stdout.Close()
	}
	if process.command != nil && process.command.Process != nil {
		if err := process.command.Process.Kill(); err != nil {
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) {
				return fmt.Errorf("worker process close: %w", err)
			}
		}
	}
	return nil
}

func readIPCFrameWithContext(ctx context.Context, reader io.Reader) ([]byte, error) {
	result := make(chan struct {
		payload []byte
		err     error
	}, 1)
	go func() {
		payload, err := ReadIPCFrame(reader)
		result <- struct {
			payload []byte
			err     error
		}{payload: payload, err: err}
	}()
	select {
	case value := <-result:
		return value.payload, value.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func writeAll(writer io.Writer, payload []byte) (int, error) {
	total := 0
	for total < len(payload) {
		n, err := writer.Write(payload[total:])
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

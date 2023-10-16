clownf_agent: clownf_agent.go clownf_constant.go
	go build -mod=vendor -ldflags=-linkmode=external
CHECKS =


clean-local:
	rm -f $(CHECKS) clownf_agent

all: all-am clownf_agent $(CHECKS)

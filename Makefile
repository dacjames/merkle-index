.PHONY: init.gitbook
init.gitbook:
	npm install gitbook-cli -g
	gitbook init docs

.PHONY: docs.serve
docs.serve:
	nohup gitbook serve docs & echo $! > run/gitbook.pid

.PHONY: docs.open
docs.open: docs.serve
	open http://localhost:4000

.PHONY: docs.stop
docs.stop:
	pkill -F run/gitbook.pid

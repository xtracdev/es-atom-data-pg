container:
	GOOS=linux GOARCH=386 CGO_ENABLED=0 go build -o ecsatomdata
	docker build -t xtracdev/ecsatomdata:latest .

clean:
	rm -f ecsatomdata

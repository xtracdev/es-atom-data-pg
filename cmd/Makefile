container:
	GOOS=linux GOARCH=386 CGO_ENABLED=0 go build -o esatomdatapg
	docker build -t xtracdev/esatomdatapg:latest .

clean:
	rm -f esatomdatapg

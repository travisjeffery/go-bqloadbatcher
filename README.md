# go-bqloadbatcher

A BigQuery client to batch and load data with Go (Golang).

## Example

``` go
loader, err := bqloadbatcher.New(&Options{
	ProjectID:     os.Getenv("PROJECT_ID"),
	Email:         os.Getenv("EMAIL"),
	PEM:           []byte(os.Getenv("PEM")),
    BatchDir:      "/tmp",
    BatchDuration: time.Minute * 5,
})
log.Check(err)

loader.Input() <- row{
    DatasetID: "yout-dataset,"
    TableID: "your-table",
    Data: yourJSON,
}
```

[GoDocs](https://godoc.org/github.com/travisjeffery/go-bqloadbatcher). 

## Author

Travis Jeffery

- [Twitter](http://twitter.com/travisjeffery)
- [Medium](http://medium.com/@travisjeffery)
- [Homepage](http://travisjeffery.com)

## License

MIT

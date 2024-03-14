# kcatpub
Publishes the output of kcat in JSON format.

## Usage
Export messages in JSON envelope and parse payload as JSON:
```shell
kcat -b localhost:9092 -t source-topic -J -e | jq -c '.payload=(if .payload==null then null else (.payload|fromjson) end)' > output.json
```
You can publish messages in `output.json` by:
```shell
kcatpub -b localhost:1092 -f output.json -t destination-topic
```

You can also use the `--session` option to store or resume a session with a specific name. If you don't provide a session name, it will default to `default`, but previous session is not continued. 
This is useful when a large file is being processed and an error occured or program interrupted with `CTRL+C`.
```shell
kcatpub -b localhost:1092 -f output.json -t destination-topic -s my-session
```

To see available options, run:
```shell
kcatpub -h
```

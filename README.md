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

To see available options, run:
```shell
kcatpub -h
```

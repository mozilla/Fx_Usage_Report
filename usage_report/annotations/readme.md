# Annotations Structure/Format

Annotations should be json files in the following structure for [ensemble transposer](https://github.com/mozilla/ensemble-transposer) to read: 

```json
{
  "country1": [
    {
      "annotation": {
        "plot/metric1 name": "annotation text",
        "plot/metric2 name": "etc"
      },
      "date": "some_date"
    },
    {
      "annotation": {
        "etc": "etc"
      },
      "date": "etc"
    }
  ],
  "country2": [
    "etc"
  ]
}
```

The keys "date" and "annotation" should always be named as such.

Note on formatting for human readability: json files can be human-readable formatted using [jq](https://stedolan.github.io/jq/), with the following command: 

```
jq --sort-keys . original.json > formatted.json
```
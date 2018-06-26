# Data Format 

This job will first process the metrics into pandas dataframes with the following fields: 

`submission_date_s3|country| metric1| metric2| etc.|`

or 

`submission_date_s3|country| metric| dimension| value|`

However, because Ensemble requires the data to be in a specific Ensemble JSON format, the data is kept in a different reshaped form: 

```
{
  "Germany": [
    {
      "date": "2017-01-01",
      "metrics": {
        "YAU": 999,
        "etc": "etc",
        "locale": {
          "DE": 0.99,
          "etc": "etc"
        }
      }
    },
    {
      "date": "etc",
      "metrics": {
        "etc": "etc"
      }
    }
  ],
  "United States": [
    {
      "date": "etc",
      "metrics": {
        "etc": "etc"
      }
    }
  ]
}
```

The job will use the processed pandas tables to update the Ensemble JSON kept in the S3 bucket. 
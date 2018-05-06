# Annotations Format

Annotations should be json files in the following format for ensemble transposer to read: 

```json
{
	"country1": [
		{"date": "some_date",
		 "annotation":{
		 	"plot/metric1 name": "annotation text", 
			"plot/metric2 name": "etc", 
		 		}
		 },
		{"date":"etc",
		 "annotation":{
		 	"etc":"etc",
		}
		 }
	],
	"country2": ["etc"]
}
```

The keys "date" and "annotation" should always be named as such.
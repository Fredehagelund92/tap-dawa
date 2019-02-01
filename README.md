# tap-dawa

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from (https://dawa.aws.dk)
- Extracts the following resources:
  - (https://dawa.aws.dk)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


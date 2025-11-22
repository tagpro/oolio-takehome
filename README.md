# Order Food Online

This is a food ordering tool that allows the client to place an order. 
The requirements are found in [README_ORIGINAL.md](./README_ORIGINAL.md).

## Installation

There are two steps to run this application

### Run the pre-compute tool

We have a big dataset for promocodes spread across multiple files. What we want to do is to figure out all the valid codes and save them in a file for our API server to use. 

To run the pre-compute tool, you need a directory with files containing the promocodes. 
We assume that the files are unzipped/gunzipped.

Directory structure (the files can be named anything):
```
promocodes/
├── file1.txt
├── file2.txt
└── file3.txt
```

Running the tool:

```bash
# --input is the directory containing the files with promocodes
# --output is the file to save the valid codes
go run cmd/precompute/main.go --input ./couponcodes --output ./valid_codes.txt
```

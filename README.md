# Sample ETL for CSV files

Load, filter, transform and save a csv file.

## Install dependencies

```bash
npm install
```

## Run the application

```bash
node index.js -u https://cdn.sample.net/backend-test-data.csv.gz
[+] Tar stream entry end.
[+] All tar stream entries read
[+] Processed the backend-test-data.csv.gz file!
[+] Finished pipeline
```

Note: the input file must be a remote tar gzipped csv file.

The `downloads` folder contains the output files.

Sample output file:

```csv
TransactionId,EventName,SiteId,SiteSectionId,PlatformGroup
1658738521217534461-o283b,defaultImpression,621717,1819063,STB/VOD
1658736650288541071-o2e9c,firstQuartile,,,STB/VOD
1658739543674790708-e9b0e,firstQuartile,1022533,16964219,OTT-Amazon Fire TV
1658738004078468029-k91dc,firstQuartile,621717,1819063,STB/VOD
1658736737737184856-e96d1,firstQuartile,621717,1819065,STB/VOD
```

## Tests

```bash
npm test
```